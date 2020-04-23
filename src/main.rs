#![warn(rust_2018_idioms)]

use std::{env, error::Error};
use tokio::net::TcpListener;
use tokio::stream::StreamExt;
use gst::prelude::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use gst_app::AppSink;
use gst::{Bin, Element};
use std::sync::mpsc;
use std::ops::AddAssign;
use log::*;

extern crate gstreamer_app as gst_app;
extern crate gstreamer as gst;


#[tokio::main]
async fn main() {
    // Make a multistream that will allow to send VideoStreamBuffers to many clients
    let shared_multi_stream =
        SharedMultiStreamWithHeader::<VideoStreamBuffer>::new();

    // mutex for signaling whether the program should stop running
    let running_mutex = Arc::new(Mutex::new(true));

    // Start the gstreamer thread which will run the gstreamer pipeline and send VideoStreamBuffers
    // over the multistream. The thread will stop when the stop condition will be set via
    // running_mutex
    let gstreamer_thread_handle = start_gstreamer_thread(
        shared_multi_stream.clone(), running_mutex.clone());

    // Start the tokio HTTP server, while also catching CTRL-C signal
    tokio::select! {
        // pass the multistream to the http server, for letting clients to
        // access the VideoStreamBuffers
        _ = start_http_server(shared_multi_stream) => {}
        _ = tokio::signal::ctrl_c() => {
            info!("\nGot Ctrl+C: Stopping stream\n");
            // Signal other threads that the program should stop
            *running_mutex.lock().unwrap() = false;
        }
    }

    // Wait for closing the gstreamer pipeline correctly
    let _res = gstreamer_thread_handle.join();
}


#[derive(Debug, Clone)]
struct VideoStreamBuffer {
    stream_ended: bool,
    buffer: Vec<u8>,
}

type BoolMutex = Arc<Mutex<bool>>;

struct Pub<T> {
    alive: bool,
    sender: mpsc::Sender<T>,
    new_publisher: bool,
}

struct Sub<T> {
    receiver: mpsc::Receiver<T>,
}

// impl Drop for Sub<T> {
//     fn drop(&mut self) {
//
//     }
// }

/// A MultiStreamWithHeader implements a simple Pub/Sub protocol of T messages
/// The initial messages of the stream contains an header, which must be
/// available for every subscriber
struct MultiStreamWithHeader<T> {
    /// inital header of the stream
    header: Vec::<T>,
    /// Publishes maintained by this multi stream.
    publishers: Vec::<Pub::<T>>,
}

impl<T> MultiStreamWithHeader<T> where T: std::clone::Clone {
    pub fn new() -> MultiStreamWithHeader<T> {
        MultiStreamWithHeader {
            header: Vec::<T>::new(),
            publishers: Vec::<Pub::<T>>::new(),
        }
    }

    pub fn add_to_header(&mut self, msg: T) {
        self.header.push(msg);
    }

    fn send_header_msgs(publisher: &mut Pub<T>, header: &Vec::<T>) {
        for header_msg in header {
            let result = publisher.sender.send(header_msg.clone());
            if result.is_err() {
                warn!("error while sending header over multistream");
            }
        }
    }

    fn send_header_msgs_if_publisher_is_new(publisher: &mut Pub<T>, header: &Vec::<T>) {
        if publisher.new_publisher {
            MultiStreamWithHeader::send_header_msgs(publisher, header);
            publisher.new_publisher = false;
        }
    }

    /// Publish a **msg** to all the subscribers maintained by this multi stream
    pub fn publish(&mut self, msg: T) {
        for publisher in &mut self.publishers {
            MultiStreamWithHeader::send_header_msgs_if_publisher_is_new(publisher, &self.header);
            let result = publisher.sender.send(msg.clone());
            if result.is_err() {
                info!("Unable to send video packet over multistream, client disconnected");
                publisher.alive = false;
            }
        }
        self.publishers.retain(|publisher| publisher.alive);
    }

    /// Create a new subscriber
    pub fn subscribe(&mut self) -> Sub<T> {
        let (sender, receiver) = mpsc::channel();

        let new_pub = Pub::<T> {
            alive: true,
            sender,
            new_publisher: true,
        };
        let new_sub = Sub::<T> {
            receiver,
        };

        self.publishers.push(new_pub);

        new_sub
    }
}

/// A thread-safe MultiStreamWithHeader
#[derive(Clone)]
struct SharedMultiStreamWithHeader<T> {
    multistream: Arc::<Mutex::<MultiStreamWithHeader::<T>>>
}

impl<T> SharedMultiStreamWithHeader<T> where T: std::clone::Clone {
    pub fn new() -> SharedMultiStreamWithHeader<T> {
        SharedMultiStreamWithHeader::<T> {
            multistream: Arc::new(Mutex::new(MultiStreamWithHeader::<T>::new()))
        }
    }

    pub fn add_to_header(&self, msg: T) {
        self.multistream.lock().unwrap().add_to_header(msg);
    }

    pub fn publish(&self, msg: T) {
        self.multistream.lock().unwrap().publish(msg);
    }

    pub fn subscribe(&mut self) -> Sub<T> {
        let sub = self.multistream.lock().unwrap().subscribe();
        sub
    }
}

fn start_gstreamer_thread(sender: SharedMultiStreamWithHeader<VideoStreamBuffer>, running_mutex: BoolMutex) -> std::thread::JoinHandle<()> {
    // get the pipeline description
    // we expect the pipeline description to be something like the following:
    //
    // ```
    // v4l2src device="/dev/video2" ! video/x-raw,width=640,height=480 ! decodebin !
    // videoconvert ! queue ! vp8enc  ! webmmux ! appsink name=mysink
    // ```
    //
    // skip the first 2 argv: executable name and server address:port

    let pipeline_words: Vec<String> = env::args().into_iter().skip(2).collect();
    let pipeline_str = pipeline_words.join(" ");

    // Spawn the gstreamer thread that will run the pipeline
    std::thread::spawn(move || {
        run_gstreamer_pipeline(sender, &pipeline_str, running_mutex);
    })
}

/// GStreamer AppSink will invoke callbacks. In order to access elements of the pipeline
/// in the callbacks, the GStreamerCustomData will be passed around
#[derive(Debug)]
struct GStreamerCustomData {
    appsink: AppSink,
}

impl GStreamerCustomData {
    fn new(appsink: &AppSink) -> GStreamerCustomData {
        GStreamerCustomData {
            appsink: appsink.clone(),
        }
    }
}


/// Run the gstreamer pipeline. GStreamer will parse the pipeline defined in **pipeline_str**
/// and then start it. The pipeline will be let run until the **running** condition will be set
/// to false
fn run_gstreamer_pipeline(sender: SharedMultiStreamWithHeader<VideoStreamBuffer>,
                          pipeline_str: &str, running: BoolMutex) {
    gst::init().unwrap();

    let parsed_element: Element = gst::parse_launch(pipeline_str)
        .expect("Could not parse the pipeline description");

    let pipeline: Bin = parsed_element.dynamic_cast::<Bin>()
        .expect("The pipeline description did not result in a pipeline");

    let sink_name = "webmsink";
    let appsink: AppSink = pipeline.get_by_name(sink_name)
        .expect(&format!("Could not find a '{}' element in the pipeline", sink_name))
        .dynamic_cast::<AppSink>().expect("The 'webmsink' element is not an appsink");

    let data: Arc<Mutex<GStreamerCustomData>> =
        Arc::new(Mutex::new(GStreamerCustomData::new(&appsink)));
    let final_message_sender: SharedMultiStreamWithHeader<VideoStreamBuffer> = sender.clone();
    let header_counter = Arc::new(Mutex::new(0u32));

    // enable callbacks
    appsink.set_emit_signals(true);

    // Setup the callback that will be called every time a new sample will be available
    appsink.connect_new_sample(move |_| {
        let appsink = {
            let data = data.lock().unwrap();
            data.appsink.clone()
        };

        // get the new sample
        let pull_result = appsink.pull_sample();

        match pull_result {
            Ok(sample) => {
                let buffer: &gst::BufferRef = sample.get_buffer().expect("No buffer in sample!");
                let mut buffer_bytes = std::vec![0u8; buffer.get_size()];

                buffer.copy_to_slice(0, buffer_bytes.as_mut_slice())
                    .expect("Unable to copy sample buffer");

                let new_video_buffer = VideoStreamBuffer {
                    stream_ended: false,
                    buffer: buffer_bytes,
                };

                if *header_counter.lock().unwrap() < 2 {
                    // The first 2 sample belong to the header
                    sender.add_to_header(new_video_buffer);
                    header_counter.lock().unwrap().add_assign(1);
                } else {
                    // send the new video buffer
                    sender.publish(new_video_buffer);
                }
            }
            Err(error) => {
                error!("Unable to pool buffer from appsink {}", error);
            }
        }

        Ok(gst::FlowSuccess::Ok)
    });

    // start the pipeline
    pipeline
        .set_state(gst::State::Playing)
        .expect("Unable to set the pipeline to the `Playing` state");

    // The pipeline will run until the program ending was requested
    while *running.lock().expect("unable to lock mutex") {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Send a dead pill to all subscribers, indicating that the stream ended
    final_message_sender.publish(VideoStreamBuffer {
        stream_ended: true,
        buffer: std::vec![],
    });

    info!("Gstreamer thread finished.")
}


/// Start a simple HTTP server
async fn start_http_server(
    mut shared_multi_stream: SharedMultiStreamWithHeader::<VideoStreamBuffer>)
        -> Result<(), Box<dyn Error>> {

    let addr_port_pair = env::args()
        .nth(1).expect("Define listening address, e.g. 127.0.0.1:9090");
    let mut server = TcpListener::bind(&addr_port_pair)
        .await.expect(&format!("Unable to bind to {}", addr_port_pair));
    println!("Listening on: {}", addr_port_pair);


    let mut incoming = server.incoming();

    // wait for new HTTP requests
    while let Some(Ok(stream)) = incoming.next().await {

        // create a new subscriber to the multistream, which will receive the VideoStreamBuffers
        let new_sub = shared_multi_stream.subscribe();

        // process the HTTP request and serve the video in a tokio task
        tokio::spawn(async move {
            if let Err(e) = process(new_sub, stream).await {
                error!("failed to process connection; error = {}", e);
            }
        });
    }

    Ok(())
}


/// process the HTTP request and serve the video
async fn process(videobuffer_receiver: Sub<VideoStreamBuffer>,
                 mut stream: tokio::net::TcpStream) -> Result<(), Box<dyn Error>> {
    info!("Got new HTTP request");

    // Consume request headers
    let mut read_data = Vec::new();
    stream.read(read_data.as_mut_slice()).await?;


    // Manually write the necessary HTTP response headers, specifying the webm content type
    stream.write("HTTP/1.1 200 OK\r\nContent-Type: video/webm\n\n".as_bytes())
        .await.expect("Unable to write HTTP response");

    // Receive all the VideoStreamBuffers and write them over TCP
    loop {
        let msg_res =
            videobuffer_receiver.receiver.try_recv();

        match msg_res {
            Ok(msg) => {
                if msg.stream_ended {
                    info!("Stream ended. Closing connection");
                    break;
                }
                let written = stream.write(msg.buffer.as_slice()).await;
                if written.is_err() || written.unwrap() < msg.buffer.len() {
                    error!("connection closed by client");
                    break;
                }
            }
            Err(recv_err) => {
                error!("Error while receiving VideoStreamBuffer from multistream {}", recv_err)
            }
        }
    }

    Ok(())
}

