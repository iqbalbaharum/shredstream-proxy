use std::{
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll},
    thread::JoinHandle,
    time::Duration,
};

use crossbeam_channel::Receiver;
use futures_core::Stream;
use jito_protos::shredstream::{
    shredstream_proxy_server::{ShredstreamProxy, ShredstreamProxyServer},
    Entry as PbEntry, SubscribeEntriesRequest,
};
use log::{debug, error, info};
use std::sync::Arc as StdArc;
use tokio::{
    net::UnixListener,
    sync::broadcast::{Receiver as BroadcastReceiver, Sender},
};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;

#[derive(Debug, Clone)]
pub struct ShredstreamProxyService {
    entry_sender: Arc<Sender<PbEntry>>,
}

pub fn start_grpc_server(
    addr: SocketAddr,
    entry_sender: Arc<Sender<PbEntry>>,
    exit: Arc<AtomicBool>,
    shutdown_receiver: Receiver<()>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let server_handle = runtime.spawn(async move {
            info!("starting server on {:?}", addr);
            tonic::transport::Server::builder()
                .add_service(ShredstreamProxyServer::new(ShredstreamProxyService {
                    entry_sender,
                }))
                .serve(addr)
                .await
                .unwrap();
        });

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if shutdown_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
            {
                server_handle.abort();
                info!("shutting down entries server");
                break;
            }
        }
    })
}

struct UnixSocketStream {
    listener: UnixListener,
}

impl UnixSocketStream {
    fn new(listener: UnixListener) -> Self {
        Self { listener }
    }
}

impl Stream for UnixSocketStream {
    type Item = io::Result<UnixStreamWrapper>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let listener = &mut self.get_mut().listener;
        match Pin::new(listener).poll_accept(cx) {
            Poll::Ready(Ok((stream, _))) => Poll::Ready(Some(Ok(UnixStreamWrapper(stream)))),
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
pub struct UdsConnectInfo {
    peer_addr: Option<StdArc<tokio::net::unix::SocketAddr>>,
    peer_cred: Option<tokio::net::unix::UCred>,
}

pub struct UnixStreamWrapper(pub tokio::net::UnixStream);

impl tokio::io::AsyncRead for UnixStreamWrapper {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for UnixStreamWrapper {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl std::ops::Deref for UnixStreamWrapper {
    type Target = tokio::net::UnixStream;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for UnixStreamWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Connected for UnixStreamWrapper {
    type ConnectInfo = UdsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        UdsConnectInfo {
            peer_addr: self.0.peer_addr().ok().map(|addr| StdArc::new(addr)),
            peer_cred: self.0.peer_cred().ok(),
        }
    }
}

pub fn start_grpc_server_on_unix_socket(
    socket_path: PathBuf,
    entry_sender: Arc<Sender<PbEntry>>,
    exit: Arc<AtomicBool>,
    shutdown_receiver: Receiver<()>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let socket_path_clone = socket_path.clone();
        let server_handle = runtime.spawn(async move {
            let _ = std::fs::remove_file(&socket_path_clone);
            let listener = match UnixListener::bind(&socket_path_clone) {
                Ok(l) => l,
                Err(e) => {
                    error!(
                        "failed to bind to unix socket {:?}: {}",
                        socket_path_clone, e
                    );
                    return;
                }
            };
            info!("starting server on unix socket {:?}", socket_path_clone);

            let incoming = UnixSocketStream::new(listener);
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(ShredstreamProxyServer::new(ShredstreamProxyService {
                    entry_sender,
                }))
                .serve_with_incoming(incoming)
                .await
            {
                debug!("grpc server error: {:?}", e);
            }

            let _ = std::fs::remove_file(&socket_path_clone);
            info!("cleaned up unix socket {:?}", socket_path_clone);
        });

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if shutdown_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
            {
                server_handle.abort();
                info!("shutting down entries server");
                break;
            }
        }
    })
}

#[tonic::async_trait]
impl ShredstreamProxy for ShredstreamProxyService {
    type SubscribeEntriesStream = ReceiverStream<Result<PbEntry, tonic::Status>>;

    async fn subscribe_entries(
        &self,
        _request: tonic::Request<SubscribeEntriesRequest>,
    ) -> Result<tonic::Response<Self::SubscribeEntriesStream>, tonic::Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let mut entry_receiver: BroadcastReceiver<PbEntry> = self.entry_sender.subscribe();

        tokio::spawn(async move {
            while let Ok(entry) = entry_receiver.recv().await {
                match tx.send(Ok(entry)).await {
                    Ok(_) => (),
                    Err(_e) => {
                        debug!("client disconnected");
                        break;
                    }
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}
