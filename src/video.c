#include <libavformat/avformat.h>
#include <libavfilter/buffersink.h>
#include <libavfilter/buffersrc.h>
#include <libswresample/swresample.h>
#include <libavutil/audio_fifo.h>
#include <libavutil/opt.h>

#define R_NO_REMAP
#define STRICT_R_HEADERS
#include <Rinternals.h>

#define VIDEO_TIME_BASE 1000

typedef struct {
  int completed;
  AVFormatContext *demuxer;
  AVCodecContext *decoder;
  AVStream *stream;
} input_container;

typedef struct {
  int64_t pts;
  AVFormatContext *muxer;
  AVCodecContext *video_encoder;
  AVStream *video_stream;
  AVCodecContext *audio_encoder;
  AVStream *audio_stream;
  SwrContext *resampler;
  AVAudioFifo * fifo;
} output_container;

typedef struct {
  AVFilterContext *input;
  AVFilterContext *output;
  AVFilterGraph *graph;
} video_filter;

static void bail_if(int ret, const char * what){
  if(ret < 0)
    Rf_errorcall(R_NilValue, "FFMPEG error in '%s': %s", what, av_err2str(ret));
}

static void bail_if_null(void * ptr, const char * what){
  if(!ptr)
    bail_if(-1, what);
}

static void close_input_file(input_container *input){
  avcodec_close(input->decoder);
  avcodec_free_context(&(input->decoder));
  avformat_free_context(input->demuxer);
  av_free(input);
}

static AVFrame * init_output_frame(AVCodecContext *audio_encoder, int frame_size){
  AVFrame *frame = av_frame_alloc();
  bail_if_null(frame, "av_frame_alloc");
  frame->nb_samples = frame_size;
  frame->channel_layout = audio_encoder->channel_layout;
  frame->format = audio_encoder->sample_fmt;
  frame->sample_rate = audio_encoder->sample_rate;
  bail_if(av_frame_get_buffer(frame, 0), "av_frame_get_buffer");
  return frame;
}

static uint8_t ** allocate_converted_samples(AVCodecContext *output_codec_context, int frame_size){
  /*Allocate as many pointers as there are audio channels*/
  uint8_t **converted_input_samples;
  bail_if(av_samples_alloc_array_and_samples(&converted_input_samples, NULL, output_codec_context->channels,
                   frame_size, output_codec_context->sample_fmt, 0), "av_samples_alloc");
  return converted_input_samples;
}

static void convert_to_fifo(AVFrame *input_frame, output_container * output){
  int frame_size = input_frame->nb_samples;
  Rprintf("Adding frame of size %d to fifo of size %d\n", frame_size, av_audio_fifo_size(output->fifo));
  uint8_t ** converted_input_samples = allocate_converted_samples(output->audio_encoder, frame_size);
  bail_if(swr_convert(output->resampler, converted_input_samples, frame_size,
                      (const uint8_t **) input_frame->extended_data,frame_size), "swr_convert");
  bail_if(av_audio_fifo_realloc(output->fifo, av_audio_fifo_size(output->fifo) + frame_size), "av_audio_fifo_realloc");
  bail_if(av_audio_fifo_write(output->fifo, (void**) converted_input_samples, frame_size) < frame_size, "av_audio_fifo_write");
  Rprintf("New fifo size: %d\n", av_audio_fifo_size(output->fifo));
  if (converted_input_samples) {
    av_freep(&converted_input_samples[0]);
    free(converted_input_samples);
  }
  av_frame_free(&input_frame);
}

void recode_audio_stream(input_container * audio_input, output_container * output, int force_flush){
  if(audio_input->completed)
    return;
  AVPacket *pkt = av_packet_alloc();
  while(av_compare_ts(output->audio_stream->cur_dts, output->audio_encoder->time_base,
                      output->video_stream->cur_dts, output->video_stream->time_base) < 0) {
    Rprintf("Adding audio: ");
    int ret = av_read_frame(audio_input->demuxer, pkt);
    if(ret == AVERROR_EOF || force_flush < 0){
      bail_if(avcodec_send_packet(audio_input->decoder, NULL), "avcodec_send_packet (flush)");
    } else {
      bail_if(ret, "av_read_frame");
      if(pkt->stream_index != audio_input->stream->index)
        continue;
      Rprintf("Found a packet!\n");
      av_packet_rescale_ts(pkt, audio_input->stream->time_base, audio_input->decoder->time_base);
      bail_if(avcodec_send_packet(audio_input->decoder, pkt), "avcodec_send_packet (audio)");
      av_packet_unref(pkt);
    }
    int audio_eof = 0;
    do {
      AVFrame *input_frame = av_frame_alloc();
      ret = avcodec_receive_frame(audio_input->decoder, input_frame);
      if(ret == AVERROR(EAGAIN))
        break;
      if(ret == AVERROR_EOF){
        Rprintf("Encounted EOF!\n");
        audio_eof = 1;
      } else {
        bail_if(ret, "avcodec_receive_frame");
        Rprintf("Converting input frame...");
        convert_to_fifo(input_frame, output);
        Rprintf("OK!\n");
      }
      Rprintf("Current fifo size: %d\n", av_audio_fifo_size(output->fifo));
      const int max_frame_size = output->audio_encoder->frame_size;
      while(av_audio_fifo_size(output->fifo) >= max_frame_size || (audio_eof && av_audio_fifo_size(output->fifo) > 0)){
        const int output_frame_size = FFMIN(av_audio_fifo_size(output->fifo), max_frame_size);
        Rprintf("Going to read %d samples...", output_frame_size);
        AVFrame * output_frame = init_output_frame(output->audio_encoder, output_frame_size);
        bail_if(av_audio_fifo_read(output->fifo, (void **)output_frame->data, output_frame_size) < output_frame_size, "av_audio_fifo_read");
        Rprintf("OK!\n");
        output_frame->pts = output->pts;
        output->pts += output_frame->nb_samples;
        Rprintf("Sending frame with %d samples and pts %d to encoder...", output_frame->nb_samples, output_frame->pts);
        bail_if(avcodec_send_frame(output->audio_encoder, output_frame), "avcodec_send_frame (audio)");
        Rprintf("OK!\n");
        av_frame_free(&output_frame);
        Rprintf("Free ok\n");
        while(1){
          Rprintf("Receiving packet from encoder...");
          AVPacket *outpkt = av_packet_alloc();
          ret = avcodec_receive_packet(output->audio_encoder, outpkt);
          if (ret == AVERROR(EAGAIN))
            break;
          if (ret == AVERROR_EOF) {
            av_log(NULL, AV_LOG_INFO, " audio stream complete!\n");
            audio_input->completed = 1;
            return;
          }
          bail_if(ret, "avcodec_receive_packet (audio)");
          Rprintf("got packet of size %d\n", outpkt->size);
          outpkt->stream_index = output->audio_stream->index;
          av_packet_rescale_ts(outpkt, output->audio_encoder->time_base, output->audio_stream->time_base);
          Rprintf("Writing to muxer (stream %d), (size %d)...", outpkt->stream_index, outpkt->size);
          bail_if_null(outpkt->data, "data is null");
          bail_if(av_interleaved_write_frame(output->muxer, outpkt), "av_interleaved_write_frame");
          Rprintf("OK!\n");
          av_packet_free(&outpkt);
          Rprintf(".");
        }
      }
    } while(!audio_eof);
  }
  Rprintf(" (synced audio until: %d)\n", output->audio_stream->cur_dts);
}

static input_container *open_audio_input(const char *filename){
  AVFormatContext *ifmt_ctx = NULL;
  bail_if(avformat_open_input(&ifmt_ctx, filename, NULL, NULL), "avformat_open_input");
  bail_if(avformat_find_stream_info(ifmt_ctx, NULL), "avformat_find_stream_info");

  /* Try all input streams */
  for (int i = 0; i < ifmt_ctx->nb_streams; i++) {
    AVStream *stream = ifmt_ctx->streams[i];
    if(stream->codecpar->codec_type != AVMEDIA_TYPE_AUDIO)
      continue;
    AVCodec *codec = avcodec_find_decoder(stream->codecpar->codec_id);
    bail_if_null(codec, "avcodec_find_decoder");
    AVCodecContext *decoder = avcodec_alloc_context3(codec);
    bail_if(avcodec_parameters_to_context(decoder, stream->codecpar), "avcodec_parameters_to_context");
    bail_if(avcodec_open2(decoder, codec, NULL), "avcodec_open2 (audio)");

    /* Store relevant objects */
    input_container *out = (input_container*) av_mallocz(sizeof(input_container));
    out->demuxer = ifmt_ctx;
    out->stream = ifmt_ctx->streams[i];
    out->decoder = decoder;
    return out;
  }
  //TODO: close+free input here too
  Rf_error("No suitable audio stream found in %s", filename);
}

static void open_audio_output(output_container *container, AVCodecContext *audio_decoder){
  /* Init the encoder context */
  AVCodec *output_codec = avcodec_find_encoder(container->muxer->oformat->audio_codec);
  bail_if_null(output_codec, "Failed to find default audio codec");
  AVCodecContext *audio_encoder = avcodec_alloc_context3(output_codec);
  bail_if_null(audio_encoder, "avcodec_alloc_context3 (audio)");
  audio_encoder->channels = audio_decoder->channels;
  audio_encoder->channel_layout = av_get_default_channel_layout(audio_decoder->channels);
  audio_encoder->sample_rate = audio_decoder->sample_rate;
  audio_encoder->sample_fmt = output_codec->sample_fmts ? output_codec->sample_fmts[0] : audio_decoder->sample_fmt;
  audio_encoder->bit_rate = audio_decoder->bit_rate;
  audio_encoder->strict_std_compliance = FF_COMPLIANCE_EXPERIMENTAL;

  /* Add the audio_stream to the muxer */
  AVStream *audio_stream = avformat_new_stream(container->muxer, output_codec);
  audio_stream->time_base.den = audio_decoder->sample_rate;
  audio_stream->time_base.num = 1;
  bail_if(avcodec_open2(audio_encoder, output_codec, NULL), "avcodec_open2 (audio)");
  bail_if(avcodec_parameters_from_context(audio_stream->codecpar, audio_encoder), "avcodec_parameters_from_context (audio)");

  /* Init the resampler */
  SwrContext *resampler = swr_alloc_set_opts(NULL,
                                        av_get_default_channel_layout(audio_encoder->channels),
                                        audio_encoder->sample_fmt,
                                        audio_encoder->sample_rate,
                                        av_get_default_channel_layout(audio_decoder->channels),
                                        audio_decoder->sample_fmt,
                                        audio_decoder->sample_rate,
                                        0, NULL);
  bail_if_null(resampler, "swr_alloc_set_opts");
  bail_if(audio_encoder->sample_rate != audio_decoder->sample_rate, "assert matching sample rates");
  bail_if(swr_init(resampler), "swr_init");

  /* Init FIFO */
  AVAudioFifo *fifo = av_audio_fifo_alloc(audio_decoder->sample_fmt,
                                          audio_decoder->channels, 1);

  /* Store outputs in the container */
  container->audio_encoder = audio_encoder;
  container->audio_stream = audio_stream;
  container->resampler = resampler;
  container->fifo = fifo;
}

static output_container *open_output_file(const char *filename, int width, int height, AVCodec *codec, input_container *audio_input){
  /* Init container context (infers format from file extension) */
  AVFormatContext *muxer = NULL;
  avformat_alloc_output_context2(&muxer, NULL, NULL, filename);
  bail_if_null(muxer, "avformat_alloc_output_context2");

  /* Init video encoder */
  AVCodecContext *video_encoder = avcodec_alloc_context3(codec);
  bail_if_null(video_encoder, "avcodec_alloc_context3");
  video_encoder->height = height;
  video_encoder->width = width;
  video_encoder->time_base.num = 1;
  video_encoder->time_base.den = VIDEO_TIME_BASE;
  video_encoder->framerate = av_inv_q(video_encoder->time_base);
  video_encoder->gop_size = 5;
  video_encoder->max_b_frames = 1;

  /* Try to use codec preferred pixel format, otherwise default to YUV420 */
  video_encoder->pix_fmt = codec->pix_fmts ? codec->pix_fmts[0] : AV_PIX_FMT_YUV420P;
  if (muxer->oformat->flags & AVFMT_GLOBALHEADER)
    video_encoder->flags |= AV_CODEC_FLAG_GLOBAL_HEADER;

  /* Open the codec, and set some x264 preferences */
  bail_if(avcodec_open2(video_encoder, codec, NULL), "avcodec_open2");
  if (codec->id == AV_CODEC_ID_H264){
    bail_if(av_opt_set(video_encoder->priv_data, "preset", "slow", 0), "Set x264 preset to slow");
    //bail_if(av_opt_set(video_encoder->priv_data, "crf", "0", 0), "Set x264 quality to lossless");
  }

  /* Start a video stream */
  AVStream *video_stream = avformat_new_stream(muxer, codec);
  bail_if_null(video_stream, "avformat_new_stream");
  bail_if(avcodec_parameters_from_context(video_stream->codecpar, video_encoder), "avcodec_parameters_from_context");

  /* Open output file file */
  if (!(muxer->oformat->flags & AVFMT_NOFILE))
    bail_if(avio_open(&muxer->pb, filename, AVIO_FLAG_WRITE), "avio_open");
  bail_if(avformat_write_header(muxer, NULL), "avformat_write_header");

  /* Store relevant objects */
  output_container *out = (output_container*) av_mallocz(sizeof(output_container));
  out->muxer = muxer;
  out->video_stream = video_stream;
  out->video_encoder = video_encoder;

  /* Add audio stream if needed */
  if(audio_input != NULL)
    open_audio_output(out, audio_input->decoder);

  //print info and return
  av_dump_format(muxer, 0, filename, 1);
  return out;
}

static void close_output_file(output_container *output){
  bail_if(av_write_trailer(output->muxer), "av_write_trailer");
  if (!(output->muxer->oformat->flags & AVFMT_NOFILE))
    avio_closep(&output->muxer->pb);
  avcodec_close(output->video_encoder);
  avcodec_free_context(&(output->video_encoder));
  if(output->audio_encoder != NULL) {
    avcodec_close(output->audio_encoder);
    avcodec_free_context(&(output->video_encoder));
    swr_free(&output->resampler);
    av_audio_fifo_free(output->fifo);
  }
  avformat_free_context(output->muxer);
  av_free(output);
}

static AVFrame * read_single_frame(const char *filename){
  AVFormatContext *ifmt_ctx = NULL;
  bail_if(avformat_open_input(&ifmt_ctx, filename, NULL, NULL), "avformat_open_input");
  bail_if(avformat_find_stream_info(ifmt_ctx, NULL), "avformat_find_stream_info");

  /* Try all input streams */
  for (int i = 0; i < ifmt_ctx->nb_streams; i++) {
    AVStream *stream = ifmt_ctx->streams[i];
    if(stream->codecpar->codec_type != AVMEDIA_TYPE_VIDEO)
      continue;
    AVCodec *codec = avcodec_find_decoder(stream->codecpar->codec_id);
    bail_if_null(codec, "avcodec_find_decoder");
    AVCodecContext *codec_ctx = avcodec_alloc_context3(codec);
    bail_if(avcodec_parameters_to_context(codec_ctx, stream->codecpar), "avcodec_parameters_to_context");
    codec_ctx->framerate = av_guess_frame_rate(ifmt_ctx, stream, NULL);
    bail_if(avcodec_open2(codec_ctx, codec, NULL), "avcodec_open2");
    int ret;
    AVPacket *pkt = av_packet_alloc();
    AVFrame *picture = av_frame_alloc();
    do {
      ret = av_read_frame(ifmt_ctx, pkt);
      if(ret == AVERROR_EOF){
        bail_if(avcodec_send_packet(codec_ctx, NULL), "flushing avcodec_send_packet");
      } else {
        bail_if(ret, "av_read_frame");
        if(pkt->stream_index != i){
          av_packet_unref(pkt);
          continue; //wrong stream
        }
        bail_if(avcodec_send_packet(codec_ctx, pkt), "avcodec_send_packet");
      }
      av_packet_unref(pkt);
      int ret2 = avcodec_receive_frame(codec_ctx, picture);
      if(ret2 == AVERROR(EAGAIN))
        continue;
      bail_if(ret2, "avcodec_receive_frame");
      av_packet_free(&pkt);
      avcodec_close(codec_ctx);
      avcodec_free_context(&codec_ctx);
      avformat_close_input(&ifmt_ctx);
      avformat_free_context(ifmt_ctx);
      return picture;
    } while(ret == 0);
  }
  Rf_error("No suitable stream or frame found");
}

static video_filter *open_filter(AVFrame * input, enum AVPixelFormat fmt, const char *filter_spec){

  /* Create a new filter graph */
  AVFilterGraph *filter_graph = avfilter_graph_alloc();

  /* Initiate source filter */
  char input_args[512];
  snprintf(input_args, sizeof(input_args),
           "video_size=%dx%d:pix_fmt=%d:time_base=%d/%d:pixel_aspect=%d/%d",
           input->width, input->height, input->format, 1, VIDEO_TIME_BASE,
           input->sample_aspect_ratio.num, input->sample_aspect_ratio.den);
  AVFilterContext *buffersrc_ctx = NULL;
  bail_if(avfilter_graph_create_filter(&buffersrc_ctx, avfilter_get_by_name("buffer"), "in",
                                       input_args, NULL, filter_graph), "avfilter_graph_create_filter (input_args)");

  /* Initiate sink filter */
  AVFilterContext *buffersink_ctx = NULL;
  bail_if(avfilter_graph_create_filter(&buffersink_ctx, avfilter_get_by_name("buffersink"), "out",
                                       NULL, NULL, filter_graph), "avfilter_graph_create_filter (output)");

  /* I think this convert output YUV420P (copied from ffmpeg examples/transcoding.c) */
  bail_if(av_opt_set_bin(buffersink_ctx, "pix_fmts",
                         (uint8_t*)&fmt, sizeof(fmt),
                         AV_OPT_SEARCH_CHILDREN), "av_opt_set_bin");


  /* Endpoints for the filter graph. */
  AVFilterInOut *outputs = avfilter_inout_alloc();
  AVFilterInOut *inputs = avfilter_inout_alloc();
  outputs->name = av_strdup("in");
  outputs->filter_ctx = buffersrc_ctx;
  outputs->pad_idx = 0;
  outputs->next = NULL;
  inputs->name = av_strdup("out");
  inputs->filter_ctx = buffersink_ctx;
  inputs->pad_idx = 0;
  inputs->next = NULL;

  /* Parse and init the custom user filter */
  bail_if(avfilter_graph_parse_ptr(filter_graph, filter_spec,
                                   &inputs, &outputs, NULL), "avfilter_graph_parse_ptr");
  bail_if(avfilter_graph_config(filter_graph, NULL), "avfilter_graph_config");
  avfilter_inout_free(&inputs);
  avfilter_inout_free(&outputs);

  video_filter *out = (video_filter*) av_mallocz(sizeof(video_filter));
  out->input = buffersrc_ctx;
  out->output = buffersink_ctx;
  out->graph = filter_graph;
  return out;
}

static void close_video_filter(video_filter *filter){
  for(int i = 0; i < filter->graph->nb_filters; i++)
    avfilter_free(filter->graph->filters[i]);
  avfilter_graph_free(&filter->graph);
  av_free(filter);
}

SEXP R_encode_video(SEXP in_files, SEXP out_file, SEXP framerate, SEXP filterstr, SEXP enc, SEXP audio){
  double duration = VIDEO_TIME_BASE / Rf_asReal(framerate);
  AVCodec *codec = NULL;
  if(Rf_length(enc)) {
    codec = avcodec_find_encoder_by_name(CHAR(STRING_ELT(enc, 0)));
  } else {
    AVOutputFormat *frmt = av_guess_format(NULL, CHAR(STRING_ELT(out_file, 0)), NULL);
    bail_if_null(frmt, "av_guess_format");
    codec = avcodec_find_encoder(frmt->video_codec);
  }
  bail_if_null(codec, "avcodec_find_encoder_by_name");
  enum AVPixelFormat pix_fmt = codec->pix_fmts ? codec->pix_fmts[0] : AV_PIX_FMT_YUV420P;

  /* Start the output video */
  AVFrame * frame = NULL;
  video_filter *filter = NULL;
  output_container *output = NULL;
  AVPacket *pkt = av_packet_alloc();
  input_container * audio_input = NULL;

  /* Open audio input */
  if(Rf_length(audio))
    audio_input = open_audio_input(CHAR(STRING_ELT(audio, 0)));

  /* Loop over input image files files */
  int len = Rf_length(in_files);
  for(int i = 0; i <= len; i++){
    if(i < Rf_length(in_files)) {
      frame = read_single_frame(CHAR(STRING_ELT(in_files, i)));
      frame->pts = i * duration;
      if(filter == NULL)
        filter = open_filter(frame, pix_fmt, CHAR(STRING_ELT(filterstr, 0)));
      bail_if(av_buffersrc_add_frame(filter->input, frame), "av_buffersrc_add_frame");
      av_frame_free(&frame);
    } else {
      bail_if_null(filter, "Faild to read any input frames");
      bail_if(av_buffersrc_add_frame(filter->input, NULL), "flushing filter");
    }

    /* Loop over frames returned by filter */
    while(1){
      AVFrame * outframe = av_frame_alloc();
      int ret = av_buffersink_get_frame(filter->output, outframe);
      if(ret == AVERROR(EAGAIN))
        break;
      if(ret == AVERROR_EOF){
        bail_if_null(output, "filter did not return any frames");
        bail_if(avcodec_send_frame(output->video_encoder, NULL), "avcodec_send_frame");
      } else {
        bail_if(ret, "av_buffersink_get_frame");
        outframe->pict_type = AV_PICTURE_TYPE_I;
        if(output == NULL)
          output = open_output_file(CHAR(STRING_ELT(out_file, 0)), outframe->width, outframe->height, codec, audio_input);
        bail_if(avcodec_send_frame(output->video_encoder, outframe), "avcodec_send_frame");
        av_frame_free(&outframe);
      }

      /* re-encode output packet */
      while(1){
        int ret = avcodec_receive_packet(output->video_encoder, pkt);
        if (ret == AVERROR(EAGAIN))
          break;
        if (ret == AVERROR_EOF){
          av_log(NULL, AV_LOG_INFO, " - video stream completed!\n");
          goto done;
        }
        bail_if(ret, "avcodec_receive_packet");
        //pkt->duration = duration; <-- may have changed by the filter!
        pkt->stream_index = output->video_stream->index;
        av_log(NULL, AV_LOG_INFO, "\rAdding frame %d at timestamp %.2fsec (%d%%)",
               (int) output->video_stream->nb_frames + 1, (double) pkt->pts / VIDEO_TIME_BASE, i * 100 / len);
        av_packet_rescale_ts(pkt, output->video_encoder->time_base, output->video_stream->time_base);
        av_log(NULL, AV_LOG_INFO, "Writing video frame of size %d\n", pkt->size);
        bail_if(av_interleaved_write_frame(output->muxer, pkt), "av_interleaved_write_frame");
        av_packet_unref(pkt);
        R_CheckUserInterrupt();
        if(audio_input != NULL)
          recode_audio_stream(audio_input, output, 0);
      }
    }
  }
  Rf_warning("Did not reach EOF, video may be incomplete");
done:
  close_video_filter(filter);
  if(audio_input != NULL){
    if(!audio_input->completed)
      recode_audio_stream(audio_input, output, 1);
    close_input_file(audio_input);
  }
  close_output_file(output);
  av_packet_free(&pkt);
  return out_file;
}
