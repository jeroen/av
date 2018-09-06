#' Create Video
#'
#' Encodes a series of images into a video.
#'
#' @export
#' @useDynLib av R_encode_video
#' @rdname av_convert
#' @param input a vector with images such as png files.
#' @param output name of the output file. File extension must correspond to a known
#' container format such as `mp4`, `mkv`, `mov`, or `flv`.
#' @param filter a string defining an ffmpeg filter graph
#' @param framerate video framerate in frames per seconds.
#' @param codec name of the video codec as listed in [av_encoders][av_encoders]. The
#' default encodes into h264 which has excellent compression and works out of the box on
#' all [modern browsers](https://caniuse.com/#search=h264) and operating systems.
#' @param audio input file with sound
#' @param progress show a progress bar counting processed input images
av_encode_video <- function(input, output = "video.mp4", framerate = 1, filter = "null", codec = "libx264", audio = NULL, progress = TRUE){
  stopifnot(length(input) > 0)
  input <- normalizePath(input, mustWork = TRUE)
  stopifnot(length(output) == 1)
  output <- normalizePath(output, mustWork = FALSE)
  stopifnot(file.exists(dirname(output)))
  stopifnot(length(framerate) == 1)
  framerate <- as.numeric(framerate)
  filter <- as.character(filter)
  codec <- as.character(codec)
  if(is.logical(progress))
    progress <- ifelse(isTRUE(progress), 32, 16)
  av_log_level(progress)
  if(length(audio)){
    stopifnot(length(audio) == 1)
    audio <- normalizePath(audio, mustWork = TRUE)
  }
  .Call(R_encode_video, input, output, framerate, filter, codec, audio)
}
