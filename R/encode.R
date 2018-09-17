#' Video Encoding
#'
#' Encodes a set of images into a video, using custom container format, codec, fps, and
#' [video filters](https://ffmpeg.org/ffmpeg-filters.html#Video-Filters).
#'
#' The container format is determined from the file extension of the output file, for
#' example `mp4`, `mkv`, `mov`, or `flv`. Most systems also support `gif` output, but
#' the compression~quality for gif is quite bad.
#' The [gifski](https://cran.r-project.org/package=gifski) package is better suited
#' for generating animated gif files.
#'
#' It is recommended to use let the encoder choose the codec. Most video formats default
#' to the `libx264` video codec which has excellent compression and works out of the box
#' on all [modern browsers](https://caniuse.com/#search=h264) and operating systems.
#'
#' @export
#' @aliases av
#' @family av
#' @name encoding
#' @useDynLib av R_encode_video
#' @rdname av_encode_video
#' @param input a vector with images such as png files. All input files should have
#' the same width and height.
#' @param output name of the output file. File extension must correspond to a known
#' container format such as `mp4`, `mkv`, `mov`, or `flv`.
#' @param filter a string defining an ffmpeg filter graph. This is the same parameter
#' as the `-vf` argument in the `ffmpeg` command line utility.
#' @param framerate video framerate in frames per seconds. This is the input fps, the
#' output fps may be different if you specify a filter that modifies speed or interpolates
#' frames.
#' @param codec name of the video codec as listed in [av_encoders][av_encoders]. The
#' default is `libx264` for most formats, which usually the best choice.
#' @param audio optional file with sounds to add to the video
#' @param verbose emit some output and a progress meter counting processed images. Must
#' be `TRUE` or `FALSE` or an integer with a valid [log_level](av_log_level).
av_encode_video <- function(input, output = "video.mp4", framerate = 24, filter = "null",
                            codec = NULL, audio = NULL, verbose = TRUE){
  stopifnot(length(input) > 0)
  input <- normalizePath(input, mustWork = TRUE)
  stopifnot(length(output) == 1)
  output <- normalizePath(output, mustWork = FALSE)
  stopifnot(file.exists(dirname(output)))
  stopifnot(length(framerate) == 1)
  framerate <- as.numeric(framerate)
  filter <- as.character(filter)
  codec <- as.character(codec)
  if(length(audio))
    audio <- normalizePath(audio, mustWork = TRUE)
  if(is.logical(verbose))
    verbose <- ifelse(isTRUE(verbose), 32, 16)
  old_log_level <- av_log_level()
  on.exit(av_log_level(old_log_level))
  av_log_level(verbose)
  .Call(R_encode_video, input, output, framerate, filter, codec, audio)
}
