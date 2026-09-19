use gstreamer::ClockTime;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, Caps, Element, ElementFactory, FlowError, GhostPad};
use gstreamer_app::{AppSrc, AppSrcCallbacks, AppStreamType};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{
        BcMedia, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe, VideoType,
    },
};
use tokio::{
    sync::{broadcast, mpsc::channel as mpsc, watch},
    task::JoinHandle,
};

use crate::{common::NeoInstance, rtsp::gst::NeoMediaFactory, AnyResult};

#[derive(Clone, Debug)]
pub enum AudioType {
    Aac,
    Adpcm(u32),
}

#[derive(Clone, Debug)]
struct StreamConfig {
    #[allow(dead_code)]
    resolution: [u32; 2],
    bitrate: u32,
    fps: u32,
    bitrate_table: Vec<u32>,
    fps_table: Vec<u32>,
    vid_type: Option<VideoType>,
    aud_type: Option<AudioType>,
}
impl StreamConfig {
    async fn new(instance: &NeoInstance, name: StreamKind) -> AnyResult<Self> {
        let (resolution, bitrate, fps, fps_table, bitrate_table) = instance
            .run_passive_task(|cam| {
                Box::pin(async move {
                    let infos = cam
                        .get_stream_info()
                        .await?
                        .stream_infos
                        .iter()
                        .flat_map(|info| info.encode_tables.clone())
                        .collect::<Vec<_>>();
                    if let Some(encode) =
                        infos.iter().find(|encode| encode.name == name.to_string())
                    {
                        let bitrate_table = encode
                            .bitrate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();
                        let framerate_table = encode
                            .framerate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();

                        Ok((
                            [encode.resolution.width, encode.resolution.height],
                            bitrate_table
                                .get(encode.default_bitrate as usize)
                                .copied()
                                .unwrap_or(encode.default_bitrate)
                                * 1024,
                            framerate_table
                                .get(encode.default_framerate as usize)
                                .copied()
                                .unwrap_or(encode.default_framerate),
                            framerate_table.clone(),
                            bitrate_table.clone(),
                        ))
                    } else {
                        Ok(([0, 0], 0, 0, vec![], vec![]))
                    }
                })
            })
            .await?;

        Ok(StreamConfig {
            resolution,
            bitrate,
            fps,
            fps_table,
            bitrate_table,
            vid_type: None,
            aud_type: None,
        })
    }

    fn update_fps(&mut self, fps: u32) {
        let new_fps = self.fps_table.get(fps as usize).copied().unwrap_or(fps);
        self.fps = new_fps;
    }
    #[allow(dead_code)]
    fn update_bitrate(&mut self, bitrate: u32) {
        let new_bitrate = self
            .bitrate_table
            .get(bitrate as usize)
            .copied()
            .unwrap_or(bitrate);
        self.bitrate = new_bitrate;
    }

    fn update_from_media(&mut self, media: &BcMedia) {
        match media {
            BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
            | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) => self.update_fps(*fps as u32),
            BcMedia::Aac(_) => {
                self.aud_type = Some(AudioType::Aac);
            }
            BcMedia::Adpcm(adpcm) => {
                self.aud_type = Some(AudioType::Adpcm(adpcm.block_size()));
            }
            BcMedia::Iframe(BcMediaIframe { video_type, .. })
            | BcMedia::Pframe(BcMediaPframe { video_type, .. }) => {
                self.vid_type = Some(*video_type);
            }
        }
    }
}

pub(super) async fn make_dummy_factory(
    use_splash: bool,
    pattern: String,
) -> AnyResult<NeoMediaFactory> {
    NeoMediaFactory::new_with_callback(move |element| {
        clear_bin(&element)?;
        if !use_splash {
            Ok(None)
        } else {
            build_unknown(&element, &pattern)?;
            Ok(Some(element))
        }
    })
    .await
}

enum ClientMsg {
    NewClient {
        element: Element,
        reply: tokio::sync::oneshot::Sender<Element>,
    },
}

/// How long the camera stream is kept open after the last RTSP client has left
const IDLE_GRACE: Duration = Duration::from_secs(30);
/// How long a new client waits for the camera to deliver its first frames
const CONFIG_TIMEOUT: Duration = Duration::from_secs(20);
/// Upper bound for the frames of one group of pictures that are kept for new clients
const GOP_MAX_FRAMES: usize = 1000;
/// Frames a slow client may lag behind before it has to resync on the next keyframe
const BROADCAST_CAPACITY: usize = 1024;
/// A client that has this many bytes queued is not reading: drop frames until the next keyframe
const MAX_QUEUED_BYTES: u64 = 6 * 1024 * 1024;

/// One frame of camera media that is shared between all RTSP clients
struct HubFrame {
    /// Increases every time the camera stream is (re)started
    epoch: u64,
    media: BcMedia,
}

impl HubFrame {
    fn is_keyframe(&self) -> bool {
        matches!(self.media, BcMedia::Iframe(_))
    }
}

/// Lets a gstreamer buffer point at the shared frame, so no client has to copy it
struct FrameData(Arc<HubFrame>);

impl AsRef<[u8]> for FrameData {
    fn as_ref(&self) -> &[u8] {
        match &self.0.media {
            BcMedia::Iframe(BcMediaIframe { data, .. })
            | BcMedia::Pframe(BcMediaPframe { data, .. }) => data.as_slice(),
            BcMedia::Aac(aac) => aac.data.as_slice(),
            BcMedia::Adpcm(adpcm) => adpcm.data.as_slice(),
            _ => &[],
        }
    }
}

/// The camera only sends one video stream per RTSP path, however many clients are watching
///
/// A camera such as the Reolink E1 cannot serve several video streams of the same kind at once:
/// starting a second one cuts off the first. So there is exactly one camera stream, which
/// this hub fans out to every RTSP client. It also keeps the last group of pictures, so a new
/// client gets a keyframe straight away instead of waiting for the next one.
struct Hub {
    frames: broadcast::Sender<Arc<HubFrame>>,
    gop: Mutex<Vec<Arc<HubFrame>>>,
    config: watch::Sender<Option<StreamConfig>>,
    clients: watch::Sender<usize>,
}

/// Keeps the camera stream running as long as it is alive
struct ClientGuard(Arc<Hub>);

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.0
            .clients
            .send_modify(|clients| *clients = clients.saturating_sub(1));
    }
}

impl Hub {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            frames: broadcast::channel(BROADCAST_CAPACITY).0,
            gop: Mutex::new(vec![]),
            config: watch::channel(None).0,
            clients: watch::channel(0).0,
        })
    }

    fn join(self: &Arc<Self>) -> ClientGuard {
        self.clients.send_modify(|clients| *clients += 1);
        ClientGuard(self.clone())
    }

    async fn wait_config(&self) -> AnyResult<StreamConfig> {
        let mut config = self.config.subscribe();
        let config = config.wait_for(|config| config.is_some()).await?.clone();
        config.ok_or_else(|| anyhow!("Camera stream config is missing"))
    }

    /// The frames since the last keyframe, and everything that is published after them
    fn subscribe(&self) -> (Vec<Arc<HubFrame>>, broadcast::Receiver<Arc<HubFrame>>) {
        let gop = self.gop.lock().unwrap();
        (gop.clone(), self.frames.subscribe())
    }

    fn publish(&self, frame: Arc<HubFrame>) {
        let mut gop = self.gop.lock().unwrap();
        if frame.is_keyframe() {
            gop.clear();
            gop.push(frame.clone());
        } else if !gop.is_empty() && gop.len() < GOP_MAX_FRAMES {
            gop.push(frame.clone());
        }
        // Having no receiver is fine
        let _ = self.frames.send(frame);
    }

    fn clear_gop(&self) {
        self.gop.lock().unwrap().clear();
    }
}

/// Waits until the number of clients satisfies `check`
async fn wait_clients(
    clients: &mut watch::Receiver<usize>,
    check: impl Fn(usize) -> bool,
) -> AnyResult<()> {
    clients.wait_for(|clients| check(*clients)).await?;
    Ok(())
}

/// Sleeps until the deadline, or forever when there is none
async fn sleep_until_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Owns the one camera stream: starts it for the first client and stops it when it has been
/// unused for a while
async fn run_hub(hub: Arc<Hub>, camera: NeoInstance, stream: StreamKind) -> AnyResult<()> {
    let name = camera.config().await?.borrow().name.clone();
    let mut clients = hub.clients.subscribe();
    let mut epoch = 0u64;
    loop {
        wait_clients(&mut clients, |clients| clients > 0).await?;
        epoch += 1;
        log::debug!("{name}::{stream}: Starting the camera stream");
        let mut stream_config = StreamConfig::new(&camera, stream).await?;
        let mut media_rx = camera.stream_while_live(stream).await?;
        hub.clear_gop();

        let mut learned = false;
        let mut frame_count = 0usize;
        let mut idle = false;
        // The camera stream is always read, even when nobody is watching: a stream that
        // is not read fills up the connection to the camera and blocks everything else on it
        let mut idle_deadline =
            (*clients.borrow_and_update() == 0).then(|| tokio::time::Instant::now() + IDLE_GRACE);
        loop {
            tokio::select! {
                media = media_rx.recv() => {
                    let Some(media) = media else {
                        log::debug!("{name}::{stream}: The camera stream ended");
                        break;
                    };
                    stream_config.update_from_media(&media);
                    frame_count += 1;
                    if !learned
                        && (frame_count > 10
                            || (stream_config.vid_type.is_some() && stream_config.aud_type.is_some()))
                    {
                        learned = true;
                        hub.config.send_replace(Some(stream_config.clone()));
                    }
                    if matches!(
                        media,
                        BcMedia::Iframe(_) | BcMedia::Pframe(_) | BcMedia::Aac(_) | BcMedia::Adpcm(_)
                    ) {
                        hub.publish(Arc::new(HubFrame { epoch, media }));
                    }
                }
                changed = clients.changed() => {
                    changed?;
                    // Give clients that are just reconnecting a chance to find the
                    // stream still running
                    idle_deadline = (*clients.borrow_and_update() == 0)
                        .then(|| tokio::time::Instant::now() + IDLE_GRACE);
                }
                _ = sleep_until_deadline(idle_deadline) => {
                    log::debug!("{name}::{stream}: Stopping the unused camera stream");
                    idle = true;
                    break;
                }
            }
        }
        drop(media_rx);
        hub.clear_gop();
        if !idle {
            // The camera stream failed, try again shortly if someone is still watching
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    let hub = Hub::new();
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn(async move {
        let name = camera.config().await?.borrow().name.clone();
        let mut hub_task = tokio::task::spawn(run_hub(hub.clone(), camera.clone(), stream));

        let handle_clients = async {
            while let Some(msg) = client_rx.recv().await {
                match msg {
                    ClientMsg::NewClient { element, reply } => {
                        log::debug!("New client for {name}::{stream}");
                        let camera = camera.clone();
                        let hub = hub.clone();
                        let name = name.clone();
                        tokio::task::spawn(async move {
                            clear_bin(&element)?;
                            let guard = hub.join();
                            let config = camera.config().await?.borrow().clone();

                            log::trace!("{name}::{stream}: Waiting for the camera stream");
                            let stream_config =
                                tokio::time::timeout(CONFIG_TIMEOUT, hub.wait_config()).await??;

                            log::trace!("{name}::{stream}: Building the pipeline");
                            // Build the right video pipeline
                            let vid_src = match stream_config.vid_type.as_ref() {
                                Some(VideoType::H264) => {
                                    let src = build_h264(&element, &stream_config)?;
                                    AnyResult::Ok(Some(src))
                                }
                                Some(VideoType::H265) => {
                                    let src = build_h265(&element, &stream_config)?;
                                    AnyResult::Ok(Some(src))
                                }
                                None => {
                                    build_unknown(&element, &config.splash_pattern.to_string())?;
                                    AnyResult::Ok(None)
                                }
                            }?;

                            // Build the right audio pipeline
                            let aud_src = match stream_config.aud_type.as_ref() {
                                Some(AudioType::Aac) => {
                                    let src = build_aac(&element, &stream_config)?;
                                    AnyResult::Ok(Some(src))
                                }
                                Some(AudioType::Adpcm(block_size)) => {
                                    let src = build_adpcm(&element, *block_size, &stream_config)?;
                                    AnyResult::Ok(Some(src))
                                }
                                None => AnyResult::Ok(None),
                            }?;

                            if let Some(app) = vid_src.as_ref() {
                                app.set_callbacks(
                                    AppSrcCallbacks::builder()
                                        .seek_data(move |_, _seek_pos| true)
                                        .build(),
                                );
                            }
                            if let Some(app) = aud_src.as_ref() {
                                app.set_callbacks(
                                    AppSrcCallbacks::builder()
                                        .seek_data(move |_, _seek_pos| true)
                                        .build(),
                                );
                            }

                            // Everything published from here on reaches this client
                            let (gop, frames_rx) = hub.subscribe();

                            log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
                            // Send the pipeline back to the factory so it can start
                            let _ = reply.send(element);

                            // Run blocking code on a seperate thread
                            // This is not an async thread
                            std::thread::spawn(move || {
                                let r = feed_client(
                                    guard,
                                    gop,
                                    frames_rx,
                                    vid_src,
                                    aud_src,
                                    &stream_config,
                                );
                                if let Err(r) = &r {
                                    log::debug!("{name}::{stream}: Client stopped: {r:?}");
                                }
                                r
                            });
                            AnyResult::Ok(())
                        });
                    }
                }
            }
            AnyResult::Ok(())
        };

        tokio::select! {
            r = &mut hub_task => {
                r??;
            }
            r = handle_clients => {
                r?;
            }
        }
        hub_task.abort();
        AnyResult::Ok(())
    });

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(move |element| {
        let (reply, new_element) = tokio::sync::oneshot::channel();
        client_tx.blocking_send(ClientMsg::NewClient { element, reply })?;

        let element = new_element.blocking_recv()?;
        Ok(Some(element))
    })
    .await?;
    Ok((factory, thread))
}

/// Is the media of this appsrc up and able to take frames?
fn pipeline_running(appsrc: Option<&AppSrc>) -> bool {
    appsrc.is_some_and(|appsrc| {
        matches!(
            appsrc.current_state(),
            gstreamer::State::Paused | gstreamer::State::Playing
        )
    })
}

/// Sends the frames of the shared camera stream into the pipeline of one client
///
/// A pipeline can be stopped and started again by the RTSP server (that is what happens
/// between DESCRIBE and PLAY). Frames that arrive while it is stopped are dropped, and
/// after it started again nothing is sent until the next keyframe.
fn feed_client(
    _guard: ClientGuard,
    gop: Vec<Arc<HubFrame>>,
    mut frames_rx: broadcast::Receiver<Arc<HubFrame>>,
    vid_src: Option<AppSrc>,
    aud_src: Option<AppSrc>,
    stream_config: &StreamConfig,
) -> AnyResult<()> {
    const MICROSECONDS: u32 = 1000000;
    let vid_step = MICROSECONDS / stream_config.fps.max(1);
    let probe = vid_src.as_ref().or(aud_src.as_ref());

    // The frames since the last keyframe let the pipeline start at once
    let mut backlog: VecDeque<Arc<HubFrame>> = gop.into();
    let mut started = false;
    let mut epoch = None;
    let mut vid_ts = 0u32;
    let mut aud_ts = 0u32;
    loop {
        // Stop when the client has gone
        if let Some(probe) = probe {
            check_live(probe)?;
        }
        // Nothing can be pushed while the media is not running
        if !pipeline_running(probe) {
            started = false;
            std::thread::sleep(Duration::from_millis(10));
            continue;
        }

        let frame = match backlog.pop_front() {
            Some(frame) => frame,
            None => match frames_rx.blocking_recv() {
                Ok(frame) => frame,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    started = false;
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            },
        };

        if epoch != Some(frame.epoch) {
            // The camera stream has been restarted
            epoch = Some(frame.epoch);
            started = false;
        }
        if started && probe.is_some_and(|probe| probe.current_level_bytes() > MAX_QUEUED_BYTES) {
            // This client is not reading
            started = false;
        }

        let pushed = match &frame.media {
            BcMedia::Iframe(_) | BcMedia::Pframe(_) => {
                if frame.is_keyframe() && !started {
                    started = true;
                    vid_ts = 0;
                    aud_ts = 0;
                }
                if started {
                    let pushed = match vid_src.as_ref() {
                        Some(vid_src) => push_frame(vid_src, &frame, vid_ts)?,
                        None => true,
                    };
                    vid_ts += vid_step;
                    pushed
                } else {
                    true
                }
            }
            BcMedia::Aac(_) | BcMedia::Adpcm(_) => {
                if started {
                    let duration = match &frame.media {
                        BcMedia::Aac(aac) => aac.duration(),
                        BcMedia::Adpcm(adpcm) => adpcm.duration(),
                        _ => None,
                    }
                    .unwrap_or(0);
                    let pushed = match aud_src.as_ref() {
                        Some(aud_src) => push_frame(aud_src, &frame, aud_ts)?,
                        None => true,
                    };
                    aud_ts += duration;
                    pushed
                } else {
                    true
                }
            }
            _ => true,
        };
        if !pushed {
            // The pipeline is not taking frames. Start over at the next keyframe
            started = false;
        }
    }
}

/// Pushes a frame into an appsrc. Returns false when the appsrc is not taking frames
fn push_frame(appsrc: &AppSrc, frame: &Arc<HubFrame>, ts: u32) -> AnyResult<bool> {
    check_live(appsrc)?; // Stop if appsrc is dropped

    let mut buf = gstreamer::Buffer::from_slice(FrameData(frame.clone()));
    {
        let buf_mut = buf.get_mut().unwrap();
        let time = ClockTime::from_useconds(ts as u64);
        buf_mut.set_dts(time);
        buf_mut.set_pts(time);
    }

    match appsrc.push_buffer(buf) {
        Ok(_) => Ok(true),
        Err(FlowError::Flushing) => Ok(false),
        Err(e) => Err(anyhow!("Error in streaming: {e:?}")),
    }
}
fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
}

fn clear_bin(bin: &Element) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    // Clear the autogenerated ones
    for element in bin.iterate_elements().into_iter().flatten() {
        bin.remove(&element)?;
    }

    Ok(())
}

fn build_unknown(bin: &Element, pattern: &str) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Unknown Pipeline");
    let source = make_element("videotestsrc", "testvidsrc")?;
    source.set_property_from_str("pattern", pattern);
    source.set_property("num-buffers", 500i32); // Send buffers then EOS
    let queue = make_queue("queue0", 1024 * 1024 * 4)?;

    let overlay = make_element("textoverlay", "overlay")?;
    overlay.set_property("text", "Stream not Ready");
    overlay.set_property_from_str("valignment", "top");
    overlay.set_property_from_str("halignment", "left");
    overlay.set_property("font-desc", "Sans, 16");
    let encoder = make_element("jpegenc", "encoder")?;
    let payload = make_element("rtpjpegpay", "pay0")?;

    bin.add_many([&source, &queue, &overlay, &encoder, &payload])?;
    source.link_filtered(
        &queue,
        &Caps::builder("video/x-raw")
            .field("format", "YUY2")
            .field("width", 896i32)
            .field("height", 512i32)
            .field("framerate", gstreamer::Fraction::new(25, 1))
            .build(),
    )?;
    Element::link_many([&queue, &overlay, &encoder, &payload])?;

    Ok(())
}

struct Linked {
    appsrc: AppSrc,
    output: Element,
}

fn pipe_h264(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    log::debug!(
        "buffer_size: {buffer_size}, bitrate: {}",
        stream_config.bitrate
    );
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H264 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h264parse", "parser")?;
    // let stamper = make_element("h264timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h264(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h264(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph264pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_h265(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H265 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h265parse", "parser")?;
    // let stamper = make_element("h265timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h265(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h265(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph265pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_aac(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let parser = make_element("aacparse", "audparser")?;
    let decoder = match make_element("faad", "auddecoder_faad") {
        Ok(ele) => Ok(ele),
        Err(_) => make_element("avdec_aac", "auddecoder_avdec_aac"),
    }?;

    // The fallback
    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let fallback_switch = make_element("fallbackswitch", "audfallbackswitch");
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        fallback_switch.set_property("timeout", 3u64 * 1_000_000_000u64);
        fallback_switch.set_property("immediate-fallback", true);
    }

    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &queue, &parser, &decoder, &encoder])?;
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        bin.add_many([&silence, fallback_switch])?;
        Element::link_many([
            &source,
            &queue,
            &parser,
            &decoder,
            fallback_switch,
            &encoder,
        ])?;
        Element::link_many([&silence, fallback_switch])?;
    } else {
        Element::link_many([&source, &queue, &parser, &decoder, &encoder])?;
    }

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

fn build_aac(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_aac(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Adpcm pipeline");
    // Original command line
    // caps=audio/x-adpcm,layout=dvi,block_align={},channels=1,rate=8000
    // ! queue silent=true max-size-bytes=10485760 min-threshold-bytes=1024
    // ! adpcmdec
    // ! audioconvert
    // ! rtpL16pay name=pay1

    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    source.set_caps(Some(
        &Caps::builder("audio/x-adpcm")
            .field("layout", "div")
            .field("block_align", block_size as i32)
            .field("channels", 1i32)
            .field("rate", 8000i32)
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let decoder = make_element("decodebin", "auddecoder")?;
    let encoder = make_element("audioconvert", "audencoder")?;
    let encoder_out = encoder.clone();

    bin.add_many([&source, &queue, &decoder, &encoder])?;
    Element::link_many([&source, &queue, &decoder])?;
    decoder.connect_pad_added(move |_element, pad| {
        let sink_pad = encoder
            .static_pad("sink")
            .expect("Encoder is missing its pad");
        pad.link(&sink_pad)
            .expect("Failed to link ADPCM decoder to encoder");
    });

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder_out,
    })
}

fn build_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_adpcm(bin, block_size, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

#[allow(dead_code)]
fn pipe_silence(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Silence pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let sink_queue = make_queue("audsinkqueue", buffer_size)?;
    let sink = make_element("fakesink", "silence_sink")?;

    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let src_queue = make_queue("audsinkqueue", buffer_size)?;
    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &sink_queue, &sink, &silence, &src_queue, &encoder])?;

    Element::link_many([&source, &sink_queue, &sink])?;

    Element::link_many([&silence, &src_queue, &encoder])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

#[allow(dead_code)]
struct AppSrcPair {
    vid: AppSrc,
    aud: Option<AppSrc>,
}

// #[allow(dead_code)]
// /// Experimental build a stream of MPEGTS
// fn build_mpegts(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrcPair> {
//     let buffer_size = buffer_size(stream_config.bitrate);
//     log::debug!(
//         "buffer_size: {buffer_size}, bitrate: {}",
//         stream_config.bitrate
//     );

//     // VID
//     let vid_link = match stream_config.vid_format {
//         VidFormat::H264 => pipe_h264(bin, stream_config)?,
//         VidFormat::H265 => pipe_h265(bin, stream_config)?,
//         VidFormat::None => unreachable!(),
//     };

//     // AUD
//     let aud_link = match stream_config.aud_format {
//         AudFormat::Aac => pipe_aac(bin, stream_config)?,
//         AudFormat::Adpcm(block) => pipe_adpcm(bin, block, stream_config)?,
//         AudFormat::None => pipe_silence(bin, stream_config)?,
//     };

//     let bin = bin
//         .clone()
//         .dynamic_cast::<Bin>()
//         .map_err(|_| anyhow!("Media source's element should be a bin"))?;

//     // MUX
//     let muxer = make_element("mpegtsmux", "mpeg_muxer")?;
//     let rtp = make_element("rtpmp2tpay", "pay0")?;

//     bin.add_many([&muxer, &rtp])?;
//     Element::link_many([&vid_link.output, &muxer, &rtp])?;
//     Element::link_many([&aud_link.output, &muxer])?;

//     Ok(AppSrcPair {
//         vid: vid_link.appsrc,
//         aud: Some(aud_link.appsrc),
//     })
// }

// Convenice funcion to make an element or provide a message
// about what plugin is missing
fn make_element(kind: &str, name: &str) -> AnyResult<Element> {
    ElementFactory::make_with_name(kind, Some(name)).with_context(|| {
        let plugin = match kind {
            "appsrc" => "app (gst-plugins-base)",
            "audioconvert" => "audioconvert (gst-plugins-base)",
            "adpcmdec" => "Required for audio",
            "h264parse" => "videoparsersbad (gst-plugins-bad)",
            "h265parse" => "videoparsersbad (gst-plugins-bad)",
            "rtph264pay" => "rtp (gst-plugins-good)",
            "rtph265pay" => "rtp (gst-plugins-good)",
            "rtpjitterbuffer" => "rtp (gst-plugins-good)",
            "aacparse" => "audioparsers (gst-plugins-good)",
            "rtpL16pay" => "rtp (gst-plugins-good)",
            "x264enc" => "x264 (gst-plugins-ugly)",
            "x265enc" => "x265 (gst-plugins-bad)",
            "avdec_h264" => "libav (gst-libav)",
            "avdec_h265" => "libav (gst-libav)",
            "videotestsrc" => "videotestsrc (gst-plugins-base)",
            "imagefreeze" => "imagefreeze (gst-plugins-good)",
            "audiotestsrc" => "audiotestsrc (gst-plugins-base)",
            "decodebin" => "playback (gst-plugins-good)",
            _ => "Unknown",
        };
        format!(
            "Missing required gstreamer plugin `{}` for `{}` element",
            plugin, kind
        )
    })
}

#[allow(dead_code)]
fn make_dbl_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    // queue.set_property(
    //     "max-size-time",
    //     std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
    //         .unwrap_or(0),
    // );

    let queue2 = make_element("queue2", &format!("queue2_{}", name))?;
    queue2.set_property("max-size-bytes", buffer_size * 2u32 / 3u32);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue2.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    queue2.set_property("use-buffering", false);

    let bin = gstreamer::Bin::builder().name(name).build();
    bin.add_many([&queue, &queue2])?;
    Element::link_many([&queue, &queue2])?;

    let pad = queue
        .static_pad("sink")
        .expect("Failed to get a static pad from queue.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let pad = queue2
        .static_pad("src")
        .expect("Failed to get a static pad from queue2.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let bin = bin
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot convert bin"))?;
    Ok(bin)
}

fn make_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    Ok(queue)
}

fn buffer_size(bitrate: u32) -> u32 {
    // 0.1 seconds (according to bitrate) or 4kb what ever is larger
    std::cmp::max(bitrate * 2 / 8u32, 4u32 * 1024u32)
}
