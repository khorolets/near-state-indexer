use clap::Parser;
use std::env;

use bigdecimal::FromPrimitive;
use borsh::BorshSerialize;
use futures::StreamExt;
use scylla::{Session, SessionBuilder};

use tracing_subscriber::EnvFilter;

use crate::configs::Opts;
use near_indexer_primitives::views::StateChangeValueView;
use near_indexer_primitives::CryptoHash;
use near_primitives_core::account::Account;

mod configs;

// Categories for logging
const INDEXER: &str = "state_indexer";

// Database schema
// Main table to keep all the state changes happening in NEAR Protocol
// CREATE TABLE state_changes (
//     account_id varchar,
//     block_height varint,
//     block_hash varchar,
//     change_scope varchar,
//     data_key BLOB,
//     data_value BLOB,
//     PRIMARY KEY ((account_id, change_scope), block_height) -- prim key like this because we mostly are going to query by these 3 fields
// );
//
// TODO: implement it
// Map-table to store relation between block_hash-block_height and included chunk hashes
// CREATE TABLE blocks (
//     block_hash varchar,
//     bloch_height varchar PRIMARY KEY,
//     chunks set<varchar>
// );
//
async fn get_scylladb_session() -> anyhow::Result<Session> {
    tracing::debug!(target: INDEXER, "Connecting to ScyllaDB",);
    let scylla_url = env::var("SCYLLA_URL").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let scylla_keyspace = env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "state_indexer".to_string());
    let session: Session = SessionBuilder::new()
        .known_node(scylla_url)
        .use_keyspace(scylla_keyspace, false)
        .build()
        .await?;
    Ok(session)
}

async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    scylladb_session: &Session,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;
    tracing::info!(target: INDEXER, "Block height {}", block_height,);

    let futures = streamer_message.shards.into_iter().flat_map(|shard| {
        shard.state_changes.into_iter().map(|state_change_with_cause| {
            handle_state_change(state_change_with_cause, scylladb_session, block_height, block_hash)
        })
    });

    futures::future::join_all(futures).await;
    Ok(())
}

async fn handle_state_change(
    state_change: near_indexer_primitives::views::StateChangeWithCauseView,
    scylladb_session: &Session,
    block_height: u64,
    block_hash: CryptoHash,
) -> anyhow::Result<()> {
    match state_change.value {
        StateChangeValueView::DataUpdate { account_id, key, value } => {
            let key: &[u8] = key.as_ref();
            let value: &[u8] = value.as_ref();
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?, ?)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Data".to_string(),
                        Some(key.to_vec()),
                        Some(value.to_vec()),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "DataUpdate {}", account_id,);
        }
        StateChangeValueView::DataDeletion { account_id, key } => {
            let key: &[u8] = key.as_ref();
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?, NULL)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Data".to_string(),
                        Some(key.to_vec()),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "DataUpdate {}", account_id,);
        }
        StateChangeValueView::AccessKeyUpdate {
            account_id,
            public_key,
            access_key,
        } => {
            let data_key = public_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the PublicKey");
            let data_value = access_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the AccessKey");
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?, ?)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "AccessKey".to_string(),
                        Some(data_key),
                        Some(data_value),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "AccessKeyUpdate {}", account_id,);
        }
        StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
            let data_key = public_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the PublicKey");
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?, NULL)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "AccessKey".to_string(),
                        Some(data_key),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "AccessKeyUpdate {}", account_id,);
        }
        StateChangeValueView::ContractCodeUpdate { account_id, code } => {
            let code: &[u8] = code.as_ref();
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL, ?)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Contract".to_string(),
                        Some(code.to_vec()),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "ContractCodeUpdate {}", account_id,);
        }
        StateChangeValueView::ContractCodeDeletion { account_id } => {
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL, NULL)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Contract".to_string(),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "ContractCodeUpdate {}", account_id,);
        }
        StateChangeValueView::AccountUpdate { account_id, account } => {
            let value = Account::from(account)
                .try_to_vec()
                .expect("Failed to borsh-serialize the Account");
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL, ?)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Account".to_string(),
                        Some(value),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "AccountUpdate {}", account_id,);
        }
        StateChangeValueView::AccountDeletion { account_id } => {
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL, NULL)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Account".to_string(),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "AccountUpdate {}", account_id,);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();

    let mut env_filter = EnvFilter::new("state_indexer=info");

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    dotenv::dotenv().ok();

    let opts: Opts = Opts::parse();

    let config: near_lake_framework::LakeConfig = opts.to_lake_config().await;
    let (sender, stream) = near_lake_framework::streamer(config);

    // let redis_connection = get_redis_connection().await?;
    let scylladb_session = get_scylladb_session().await?;

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| handle_streamer_message(streamer_message, &scylladb_session))
        .buffer_unordered(1usize);

    while let Some(_handle_message) = handlers.next().await {}
    drop(handlers); // close the channel so the sender will stop

    // propagate errors from the sender
    match sender.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::Error::from(e)), // JoinError
    }
}
