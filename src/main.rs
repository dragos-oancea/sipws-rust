use sipws::{config, sip};

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let mut cfg = config::Config::parse();
    let codecs = cfg.validate()?;
    info!(
        public_ip = %cfg.public_ip(),
        ws_url = cfg.ws_url,
        ws_rate = cfg.ws_sample_rate,
        codecs = ?codecs,
        "starting sipws"
    );
    let cfg = Arc::new(cfg);
    let server = sip::server::SipServer::bind(cfg, codecs).await?;

    let runner = tokio::spawn(server.clone().run());
    tokio::select! {
        r = runner => { r??; }
        _ = shutdown_signal() => {
            info!(calls = server.active_calls(), "shutting down, hanging up calls");
            server.shutdown();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while server.active_calls() > 0 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
