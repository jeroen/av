#include <libavformat/avformat.h>
#include <libavfilter/buffersink.h>
#include <libavfilter/buffersrc.h>
#include <libavutil/opt.h>

#define R_NO_REMAP
#define STRICT_R_HEADERS
#include <Rinternals.h>

#define TIME_BASE 1000

typedef struct {
  AVFormatContext *fmt_ctx;
  AVCodecContext *codec_ctx;
  AVStream *stream;
} input_container;

typedef struct {
  AVFormatContext *fmt_ctx;
  AVCodecContext *video_codec_ctx;
  AVCodecContext *audio_codec_ctx;
  AVStream *video_stream;
  AVStream *audio_stream;
} output_container;

typedef struct {
  AVFilterContext *input;
  AVFilterContext *output;
  AVFilterGraph *filter_graph;
} filter_container;

static void bail_if(int ret, const char * what){
  if(ret < 0)
    Rf_errorcall(R_NilValue, "FFMPEG error in '%s': %s", what, av_err2str(ret));
}

static void bail_if_null(void * ptr, const char * what){
  if(!ptr)
    bail_if(-1, what);
}

static void close_input_file(input_container *input){
  avcodec_close(input->codec_ctx);
  avcodec_free_context(&(input->codec_ctx));
  avformat_free_context(input->fmt_ctx);
  av_free(input);
}

static input_container *open_audio_file(const char *filename){
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
    AVCodecContext *codec_ctx = avcodec_alloc_context3(codec);
    bail_if(avcodec_parameters_to_context(codec_ctx, stream->codecpar), "avcodec_parameters_to_context");
    bail_if(avcodec_open2(codec_ctx, codec, NULL), "avcodec_open2 (audio)");

    /* Store relevant objects */
    input_container *out = (input_container*) av_mallocz(sizeof(input_container));
    out->fmt_ctx = ifmt_ctx;
    out->stream = ifmt_ctx->streams[i];
    out->codec_ctx = codec_ctx;
    return out;
  }
  //TODO: close+free input here too
  Rf_error("No suitable audio stream found in %s", filename);
}

int recode_audio_stream(input_container * audio, filter_container *filter, output_container * output, int64_t max_pts){
  AVPacket *pkt = av_packet_alloc();
  Rprintf("Adding audio: ");
  while(av_compare_ts(output->audio_stream->cur_dts, output->audio_codec_ctx->time_base,
                      output->video_stream->cur_dts, output->video_stream->time_base) < 0) {
    int ret = av_read_frame(audio->fmt_ctx, pkt);
    if(ret == AVERROR_EOF || max_pts < 0){
      bail_if(avcodec_send_packet(audio->codec_ctx, NULL), "avcodec_send_packet (flush)");
    } else {
      bail_if(ret, "av_read_frame");
      if(pkt->stream_index != audio->stream->index)
        continue;
      av_packet_rescale_ts(pkt, audio->stream->time_base, audio->codec_ctx->time_base);
      bail_if(avcodec_send_packet(audio->codec_ctx, pkt), "avcodec_send_packet (audio)");
      av_packet_unref(pkt);
    }
    while(1){
      AVFrame *sample = av_frame_alloc();
      ret = avcodec_receive_frame(audio->codec_ctx, sample);
      if(ret == AVERROR(EAGAIN))
        break;
      if(ret == AVERROR_EOF){
        bail_if(av_buffersrc_add_frame(filter->input, NULL), "flushing filter");
      } else {
        bail_if(ret, "avcodec_receive_frame");
        bail_if(av_buffersrc_add_frame(filter->input, sample), "av_buffersrc_add_frame");
        av_frame_free(&sample);
      }
      while(1){
        AVFrame * outframe = av_frame_alloc();
        ret = av_buffersink_get_frame(filter->output, outframe);
        if(ret == AVERROR(EAGAIN))
          break;
        if(ret == AVERROR_EOF){
          bail_if(avcodec_send_frame(output->audio_codec_ctx, NULL), "avcodec_send_frame (flush)");
        } else {
          bail_if(ret, "avcodec_receive_frame (audio)");
          bail_if(avcodec_send_frame(output->audio_codec_ctx, outframe), "avcodec_send_frame (audio)");
          av_frame_free(&outframe);
        }
        while(1){
          ret = avcodec_receive_packet(output->audio_codec_ctx, pkt);
          if (ret == AVERROR(EAGAIN))
            break;
          if (ret == AVERROR_EOF){
            av_log(NULL, AV_LOG_INFO, " audio stream complete!\n");
            return AVERROR_EOF;
          }
          pkt->stream_index = output->audio_stream->index;
          av_packet_rescale_ts(pkt, output->audio_codec_ctx->time_base, output->audio_stream->time_base);
          bail_if(av_interleaved_write_frame(output->fmt_ctx, pkt), "av_interleaved_write_frame");
          av_packet_unref(pkt);
          Rprintf(".");
        }
      }
    }
  }
  Rprintf(" (synced audio until: %d)\n", output->audio_stream->cur_dts);
  return 0;
}

static output_container *open_output_file(const char *filename, int width, int height, AVCodec *codec, int len, AVCodecContext *audio_input){
  /* Init container context (infers format from file extension) */
  AVFormatContext *ofmt_ctx = NULL;
  avformat_alloc_output_context2(&ofmt_ctx, NULL, NULL, filename);
  bail_if_null(ofmt_ctx, "avformat_alloc_output_context2");

  /* Init video encoder */
  AVCodecContext *codec_ctx = avcodec_alloc_context3(codec);
  bail_if_null(codec_ctx, "avcodec_alloc_context3");
  codec_ctx->height = height;
  codec_ctx->width = width;
  codec_ctx->time_base.num = 1;
  codec_ctx->time_base.den = TIME_BASE;
  codec_ctx->framerate = av_inv_q(codec_ctx->time_base);
  codec_ctx->gop_size = 5;
  codec_ctx->max_b_frames = 1;

  /* Try to use codec preferred pixel format, otherwise default to YUV420 */
  codec_ctx->pix_fmt = codec->pix_fmts ? codec->pix_fmts[0] : AV_PIX_FMT_YUV420P;
  if (ofmt_ctx->oformat->flags & AVFMT_GLOBALHEADER)
    codec_ctx->flags |= AV_CODEC_FLAG_GLOBAL_HEADER;

  /* Open the codec, and set some x264 preferences */
  bail_if(avcodec_open2(codec_ctx, codec, NULL), "avcodec_open2 (video)");
  if (codec->id == AV_CODEC_ID_H264){
    bail_if(av_opt_set(codec_ctx->priv_data, "preset", "slow", 0), "Set x264 preset to slow");
    //bail_if(av_opt_set(codec_ctx->priv_data, "crf", "0", 0), "Set x264 quality to lossless");
  }

  /* Start a video stream */
  AVStream *out_stream = avformat_new_stream(ofmt_ctx, codec);
  bail_if_null(out_stream, "avformat_new_stream");
  bail_if(avcodec_parameters_from_context(out_stream->codecpar, codec_ctx), "avcodec_parameters_from_context");
  out_stream->nb_frames = len;

  /* Store relevant objects */
  output_container *out = (output_container*) av_mallocz(sizeof(output_container));
  out->fmt_ctx = ofmt_ctx;
  out->video_stream = out_stream;
  out->video_codec_ctx = codec_ctx;

  /* Start audio stream */
  if(audio_input != NULL){
    AVCodec *audio_codec = avcodec_find_encoder_by_name("aac");
    bail_if_null(audio_codec, "avcodec_find_encoder_by_name (aac)");
    AVCodecContext *audio_ctx = avcodec_alloc_context3(audio_codec);
    bail_if_null(audio_ctx, "avcodec_alloc_context3");
    audio_ctx->sample_rate = audio_input->sample_rate;
    audio_ctx->channel_layout = audio_input->channel_layout;
    audio_ctx->channels = av_get_channel_layout_nb_channels(audio_ctx->channel_layout);
    audio_ctx->sample_fmt = audio_codec->sample_fmts[0];
    audio_ctx->time_base = (AVRational){1, audio_ctx->sample_rate};
    bail_if(avcodec_open2(audio_ctx, audio_codec, NULL), "avcodec_open2 (audio input)");

    /* Start the stream */
    AVStream *audio_stream = avformat_new_stream(ofmt_ctx, audio_codec);
    bail_if_null(audio_stream, "avformat_new_stream (audio)");
    bail_if(avcodec_parameters_from_context(audio_stream->codecpar, audio_ctx), "avcodec_parameters_from_context");
    out->audio_stream = audio_stream;
    out->audio_codec_ctx = audio_ctx;
  } else {
    out->audio_stream = NULL;
    out->audio_codec_ctx = NULL;
  }

  /* Open output file file */
  if (!(ofmt_ctx->oformat->flags & AVFMT_NOFILE))
    bail_if(avio_open(&ofmt_ctx->pb, filename, AVIO_FLAG_WRITE), "avio_open");
  bail_if(avformat_write_header(ofmt_ctx, NULL), "avformat_write_header");

  //print info and return
  av_dump_format(ofmt_ctx, 0, filename, 1);
  return out;
}

static void close_output_file(output_container *output){
  bail_if(av_write_trailer(output->fmt_ctx), "av_write_trailer");
  if (!(output->fmt_ctx->oformat->flags & AVFMT_NOFILE))
    avio_closep(&output->fmt_ctx->pb);
  avcodec_close(output->video_codec_ctx);
  avcodec_free_context(&(output->video_codec_ctx));
  if (output->audio_codec_ctx != NULL) {
    avcodec_close(output->audio_codec_ctx);
    avcodec_free_context(&(output->audio_codec_ctx));
  }
  avformat_free_context(output->fmt_ctx);
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
  //TODO: close+free input here
  Rf_error("No suitable stream or frame found");
}

static filter_container *open_audio_filter(AVCodecContext *dec_ctx){

  /* Create a new filter graph */
  AVFilterGraph *filter_graph = avfilter_graph_alloc();

  if (!dec_ctx->channel_layout)
    dec_ctx->channel_layout = av_get_default_channel_layout(dec_ctx->channels);

  char input_args[512];
  snprintf(input_args, sizeof(input_args),
           "time_base=%d/%d:sample_rate=%d:sample_fmt=%s:channel_layout=0x%" PRIx64,
           dec_ctx->time_base.num, dec_ctx->time_base.den, dec_ctx->sample_rate,
           av_get_sample_fmt_name(dec_ctx->sample_fmt),
           dec_ctx->channel_layout);

  AVFilterContext *buffersrc_ctx = NULL;
  bail_if(avfilter_graph_create_filter(&buffersrc_ctx, avfilter_get_by_name("abuffer"), "in",
                                       input_args, NULL, filter_graph), "avfilter_graph_create_filter (input_args)");

  /* Initiate sink filter */
  AVFilterContext *buffersink_ctx = NULL;
  bail_if(avfilter_graph_create_filter(&buffersink_ctx, avfilter_get_by_name("abuffersink"), "out",
                                       NULL, NULL, filter_graph), "avfilter_graph_create_filter (output)");

  AVCodec *audio_codec = avcodec_find_encoder_by_name("aac");
  enum AVSampleFormat fmt = audio_codec->sample_fmts[0];
  bail_if(av_opt_set_bin(buffersink_ctx, "sample_fmts",
                         (uint8_t*)&fmt, sizeof(fmt),
                         AV_OPT_SEARCH_CHILDREN), "av_opt_set_bin (sample_fmts)");

  bail_if(av_opt_set_bin(buffersink_ctx, "channel_layouts",
                         (uint8_t*)&dec_ctx->channel_layout,
                         sizeof(dec_ctx->channel_layout), AV_OPT_SEARCH_CHILDREN), "av_opt_set_bin (channel_layouts)");

  bail_if(av_opt_set_bin(buffersink_ctx, "sample_rates",
                         (uint8_t*)&dec_ctx->sample_rate,
                         sizeof(dec_ctx->sample_rate), AV_OPT_SEARCH_CHILDREN), "av_opt_set_bin (sample_rates)");

  /* Hookup to the output filter */
  bail_if(avfilter_link(buffersrc_ctx, 0, buffersink_ctx, 0), "avfilter_link (sink)");
  bail_if(avfilter_graph_config(filter_graph, NULL), "avfilter_graph_config");

  filter_container *out = (filter_container*) av_mallocz(sizeof(filter_container));
  out->input = buffersrc_ctx;
  out->output = buffersink_ctx;
  out->filter_graph = filter_graph;
  return out;
}

static filter_container *open_video_filter(AVFrame * input, enum AVPixelFormat fmt, const char *filter_spec){

  /* Create a new filter graph */
  AVFilterGraph *filter_graph = avfilter_graph_alloc();

  /* Initiate source filter */
  char input_args[512];
  snprintf(input_args, sizeof(input_args),
           "video_size=%dx%d:pix_fmt=%d:time_base=%d/%d:pixel_aspect=%d/%d",
           input->width, input->height, input->format, 1, 1,
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

  filter_container *out = (filter_container*) av_mallocz(sizeof(filter_container));
  out->input = buffersrc_ctx;
  out->output = buffersink_ctx;
  out->filter_graph = filter_graph;
  return out;
}

static void close_filter_container(filter_container *filter){
  for(int i = 0; i < filter->filter_graph->nb_filters; i++)
    avfilter_free(filter->filter_graph->filters[i]);
  avfilter_graph_free(&filter->filter_graph);
  av_free(filter);
}

SEXP R_encode_video(SEXP in_files, SEXP out_file, SEXP framerate, SEXP filterstr, SEXP enc, SEXP audio_file){
  double duration = TIME_BASE / Rf_asReal(framerate);
  AVCodec *codec = avcodec_find_encoder_by_name(CHAR(STRING_ELT(enc, 0)));
  bail_if_null(codec, "avcodec_find_encoder_by_name");
  enum AVPixelFormat pix_fmt = codec->pix_fmts ? codec->pix_fmts[0] : AV_PIX_FMT_YUV420P;

  /* Start the output video */
  AVFrame * frame = NULL;
  filter_container *filter = NULL;
  output_container *outfile = NULL;
  AVPacket *pkt = av_packet_alloc();

  /* Open sound file if exists */
  input_container *audio_input = NULL;
  filter_container *audio_filter = NULL;
  if(Rf_length(audio_file)){
    audio_input = open_audio_file(CHAR(STRING_ELT(audio_file, 0)));
    audio_filter = open_audio_filter(audio_input->codec_ctx);
  }
  int has_audio = audio_input != NULL;
  int has_more = has_audio;

  /* Loop over input image files files */
  for(int i = 0; i <= Rf_length(in_files); i++){
    if(i < Rf_length(in_files)) {
      frame = read_single_frame(CHAR(STRING_ELT(in_files, i)));
      frame->pts = i * duration;
      if(filter == NULL)
        filter = open_video_filter(frame, pix_fmt, CHAR(STRING_ELT(filterstr, 0)));
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
        bail_if_null(outfile, "filter did not return any frames");
        bail_if(avcodec_send_frame(outfile->video_codec_ctx, NULL), "avcodec_send_frame");
      } else {
        bail_if(ret, "av_buffersink_get_frame");
        outframe->pict_type = AV_PICTURE_TYPE_I;
        if(outfile == NULL)
          outfile = open_output_file(CHAR(STRING_ELT(out_file, 0)), outframe->width, outframe->height,
                                    codec, Rf_length(in_files), audio_input ? audio_input->codec_ctx : NULL);
        bail_if(avcodec_send_frame(outfile->video_codec_ctx, outframe), "avcodec_send_frame");
        av_frame_free(&outframe);
      }

      /* re-encode output packet */
      while(1){
        int ret = avcodec_receive_packet(outfile->video_codec_ctx, pkt);
        if (ret == AVERROR(EAGAIN))
          break;
        if (ret == AVERROR_EOF){
          av_log(NULL, AV_LOG_INFO, " done!\n");
          goto done;
        }
        bail_if(ret, "avcodec_receive_packet");
        //pkt->duration = duration; <-- may have changed by the filter!
        pkt->stream_index = outfile->video_stream->index;
        Rprintf("Adding image with container-pts: %d\n", pkt->pts);
        if(has_more && recode_audio_stream(audio_input, audio_filter, outfile, pkt->pts) == AVERROR_EOF)
          has_more = 0;
        av_packet_rescale_ts(pkt, outfile->video_codec_ctx->time_base, outfile->video_stream->time_base);
        bail_if(av_interleaved_write_frame(outfile->fmt_ctx, pkt), "av_interleaved_write_frame");
        av_packet_unref(pkt);
      }
    }
    //av_log(NULL, AV_LOG_INFO, "\rFrame %d (%d%%)", i+1, (i+1) * 100 / Rf_length(in_files));
  }
  Rf_warning("Did not reach EOF, video may be incomplete");
done:
  if(has_audio){
    if(has_more)
      recode_audio_stream(audio_input, audio_filter, outfile, -1);
    close_input_file(audio_input);
    close_filter_container(audio_filter);
  }
  close_filter_container(filter);
  close_output_file(outfile);
  av_packet_free(&pkt);
  return out_file;
}

