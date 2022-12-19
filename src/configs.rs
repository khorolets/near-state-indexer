pub use clap::{Parser, Subcommand};

use near_jsonrpc_client::{methods, JsonRpcClient};
use near_lake_framework::near_indexer_primitives::types::{BlockReference, Finality};

/// NEAR Indexer for Explorer
/// Watches for stream of blocks from the chain
#[derive(Parser, Debug)]
#[clap(
    version,
    author,
    about,
    setting(clap::AppSettings::DisableHelpSubcommand),
    setting(clap::AppSettings::PropagateVersion),
    setting(clap::AppSettings::NextLineHelp)
)]
pub(crate) struct Opts {
    #[clap(long, default_value = "redis://127.0.0.1", env)]
    pub redis_url: String,
    /// Chain ID: testnet or mainnet
    #[clap(subcommand)]
    pub chain_id: ChainId,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ChainId {
    #[clap(subcommand)]
    Mainnet(StartOptions),
    #[clap(subcommand)]
    Testnet(StartOptions),
}

#[derive(Subcommand, Debug, Clone)]
pub enum StartOptions {
    FromBlock { height: u64 },
    FromInterruption,
    FromLatest,
}

impl Opts {
    // pub fn chain_id(&self) -> crate::types::primitives::ChainId {
    //     match self.chain_id {
    //         ChainId::Mainnet(_) => crate::types::primitives::ChainId::Mainnet,
    //         ChainId::Testnet(_) => crate::types::primitives::ChainId::Testnet,
    //     }
    // }

    /// Returns [StartOptions] for current [Opts]
    pub fn start_options(&self) -> &StartOptions {
        match &self.chain_id {
            ChainId::Mainnet(start_options) | ChainId::Testnet(start_options) => start_options,
        }
    }

    pub fn rpc_url(&self) -> &str {
        match self.chain_id {
            ChainId::Mainnet(_) => "https://rpc.mainnet.near.org",
            ChainId::Testnet(_) => "https://rpc.testnet.near.org",
        }
    }
}

impl Opts {
    pub async fn to_lake_config(&self) -> near_lake_framework::LakeConfig {
        let config_builder = near_lake_framework::LakeConfigBuilder::default();

        match &self.chain_id {
            ChainId::Mainnet(_) => config_builder
                .mainnet()
                .start_block_height(get_start_block_height(self).await),
            ChainId::Testnet(_) => config_builder
                .testnet()
                .start_block_height(get_start_block_height(self).await),
        }
        .build()
        .expect("Failed to build LakeConfig")
    }
}

// TODO: refactor to read from Redis once `storage` is extracted to a separate crate
async fn get_start_block_height(opts: &Opts) -> u64 {
    match opts.start_options() {
        StartOptions::FromBlock { height } => *height,
        StartOptions::FromInterruption => {
            todo!("Implement FromInterruption on ScyllaDB")
            // let redis_connection_manager = match opts.redis_client().await {
            //     Ok(connection_manager) => connection_manager,
            //     Err(err) => {
            //         tracing::warn!(
            //             target: crate::INDEXER,
            //             "Failed to connect to Redis to get last synced block, failing to the latest...\n{:#?}",
            //             err,
            //         );
            //         return final_block_height(opts).await;
            //     }
            // };
            // match redis::cmd("GET")
            //     .arg("last_indexed_block")
            //     .query_async(&mut redis_connection_manager.clone())
            //     .await
            // {
            //     Ok(last_indexed_block) => last_indexed_block,
            //     Err(err) => {
            //         tracing::warn!(
            //             target: crate::INDEXER,
            //             "Failed to get last indexer block from Redis. Failing to the latest one...\n{:#?}",
            //             err
            //         );
            //         final_block_height(opts).await
            //     }
            // }
        }
        StartOptions::FromLatest => final_block_height(opts).await,
    }
}

async fn final_block_height(opts: &Opts) -> u64 {
    tracing::debug!(target: crate::INDEXER, "Fetching final block from NEAR RPC",);
    let client = JsonRpcClient::connect(opts.rpc_url());
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = client.call(request).await.unwrap();

    latest_block.header.height
}
