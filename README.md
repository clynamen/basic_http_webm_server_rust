# Basic HTTP WebM Rust Server

This repo is the **rust** port of the [C HTTP live streaming server by sdroege](https://github.com/sdroege/http-launch)

The application starts a GStreamer pipeline, which should produce a WebM output, and serves the videostream over HTTP.

Video stream source compatibility eventually depends on GStreamer.

## Example Usage:

Call the executable with an ip address:port pair and a full gstreamer pipeline. 
The pipeline must have an appsink called **webmsink**, linked to a webmmux element.

```
cargo build
./target/debug/basic_http_webm_server_rust 127.0.0.1:9090 v4l2src device="/dev/video2" ! video/x-raw,width=640,height=480 ! decodebin ! videoconvert ! queue ! vp8enc deadline=1 ! webmmux ! appsink name=webmsink
```
