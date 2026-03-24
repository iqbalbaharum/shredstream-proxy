use std::{
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crossbeam_channel::Receiver;
use futures_core::Stream;
use log::{debug, error, info};
use sol_protos::shredstream::{
    shredstream_proxy_server::{ShredstreamProxy, ShredstreamProxyServer},
    Entry as PbEntry, Origin as ProtoOrigin, ParsedTransaction as PbParsedTransaction,
    SubscribeEntriesRequest, SubscribeParsedRequest, TradeType as ProtoTradeType,
};
use solana_metrics::datapoint_info;
use std::sync::Arc as StdArc;
use tokio::{
    net::UnixListener,
    sync::broadcast::{Receiver as BroadcastReceiver, Sender},
};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;

use crate::pumpfun_filter::PumpFunFilter;
use crate::pumpfun_parser::{Origin, ParsedTransaction, PumpFunParser, TradeType};

#[derive(Debug, Clone)]
pub struct ShredstreamProxyService {
    entry_sender: Arc<Sender<PbEntry>>,
    smart_filter: bool,
}

pub fn start_grpc_server(
    addr: SocketAddr,
    entry_sender: Arc<Sender<PbEntry>>,
    smart_filter: bool,
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
                    smart_filter,
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
    smart_filter: bool,
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
                    smart_filter,
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
    type SubscribeParsedTransactionsStream =
        ReceiverStream<Result<PbParsedTransaction, tonic::Status>>;

    async fn subscribe_entries(
        &self,
        request: tonic::Request<SubscribeEntriesRequest>,
    ) -> Result<tonic::Response<Self::SubscribeEntriesStream>, tonic::Status> {
        let smart_filter = request.into_inner().smart_filter;
        let service_smart_filter = self.smart_filter;
        let use_smart_filter = smart_filter || service_smart_filter;

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let mut entry_receiver: BroadcastReceiver<PbEntry> = self.entry_sender.subscribe();

        tokio::spawn(async move {
            while let Ok(entry) = entry_receiver.recv().await {
                if use_smart_filter {
                    if PumpFunFilter::filter_entries(&entry.entries).is_none() {
                        continue;
                    }
                }

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

    async fn subscribe_parsed_transactions(
        &self,
        request: tonic::Request<SubscribeParsedRequest>,
    ) -> Result<tonic::Response<Self::SubscribeParsedTransactionsStream>, tonic::Status> {
        let filter = request.into_inner().filter;
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let mut entry_receiver: BroadcastReceiver<PbEntry> = self.entry_sender.subscribe();

        tokio::spawn(async move {
            while let Ok(entry) = entry_receiver.recv().await {
                let entry_received_time = Instant::now();
                let slot = entry.slot;

                let parsed_txs = PumpFunParser::parse_entries(&entry.entries, &filter);
                let parse_complete_time = Instant::now();

                for parsed_tx in parsed_txs {
                    let pb_tx = convert_to_proto(&parsed_tx, entry.slot);
                    let send_start_time = Instant::now();

                    match tx.send(Ok(pb_tx)).await {
                        Ok(_) => {
                            let total_latency = entry_received_time.elapsed().as_millis() as i64;
                            let parse_latency = parse_complete_time.elapsed().as_millis() as i64;
                            let send_latency = send_start_time.elapsed().as_millis() as i64;

                            datapoint_info!(
                                "shredstream_proxy-tx_timing",
                                ("slot", slot as i64, i64),
                                ("total_latency_ms", total_latency, i64),
                                ("parse_latency_ms", parse_latency, i64),
                                ("send_latency_ms", send_latency, i64),
                            );
                        }
                        Err(_e) => {
                            debug!("client disconnected");
                            return;
                        }
                    }
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

fn convert_to_proto(tx: &ParsedTransaction, slot: u64) -> PbParsedTransaction {
    let trade_type = match tx.trade_type {
        TradeType::Unknown => ProtoTradeType::Unknown as i32,
        TradeType::PumpfunBuy => ProtoTradeType::PumpfunBuy as i32,
        TradeType::PumpfunSell => ProtoTradeType::PumpfunSell as i32,
        TradeType::PumpfunBuyExactIn => ProtoTradeType::PumpfunBuyExactIn as i32,
        TradeType::AxiomBuy => ProtoTradeType::AxiomBuy as i32,
        TradeType::AxiomSell => ProtoTradeType::AxiomSell as i32,
    };

    let origin = match tx.origin {
        Origin::Unspecified => ProtoOrigin::Unspecified as i32,
        Origin::Pumpfun => ProtoOrigin::Pumpfun as i32,
        Origin::Axiom => ProtoOrigin::Axiom as i32,
    };

    PbParsedTransaction {
        slot,
        signature: tx.signature.clone(),
        mint: tx.mint.clone(),
        signer: tx.signer.clone(),
        trade_type,
        origin,
        token_amount: tx.token_amount,
        sol_amount: tx.sol_amount,
        timestamp: tx.timestamp,
    }
}
