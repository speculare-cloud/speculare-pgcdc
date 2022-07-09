use crate::cdc::{
    connection::db_client_start,
    replication::{replication_slot_create, replication_stream_poll, replication_stream_start},
    ExtConfig,
};
use crate::forwarder::start_forwarder;
use crate::utils::ws_utils::ServerState;
use crate::{SUPERVISOR, TABLES, TABLES_LOOKUP};

use bastion::prelude::BastionContext;
use bastion::spawn;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc;

pub fn start_inner(server_state: Arc<ServerState>) {
    // Start the children in Bastion (allow for restart if fails)
    SUPERVISOR
        .children(|child| {
            child.with_exec(move |_: BastionContext| {
                trace!("Starting the replication forwarder & listener");
                let server_state = server_state.clone();

                async move {
                    // A multi-producer, single-consumer channel queue. Using 128 buffers length.
                    let (tx, rx) = mpsc::channel(128);

                    // Start listening to the Sender & forward message when receiving one
                    let handle = spawn! {
                        start_forwarder(rx, server_state).await;
                    };

                    // Form replication connection & keep the connection open
                    let client = db_client_start().await;

                    // Detect tables that we'll use to authorize or lookup with timescale
                    client.detect_tables().await;
                    trace!("Main: Allowed tables are: {:?}", &TABLES.read().unwrap());
                    #[cfg(feature = "timescale")]
                    {
                        client.detect_lookup().await;
                        trace!(
                            "Main: Tables lookup are: {:?}",
                            &TABLES_LOOKUP.read().unwrap()
                        );
                    }

                    let slot_name = uuid_readable_rs::short().replace(' ', "_").to_lowercase();
                    let lsn = replication_slot_create(&client, &slot_name).await;
                    let duplex_stream = replication_stream_start(&client, &slot_name, &lsn).await;

                    // call to panic allow us to exit this children and restart a new one
                    // in case any of the two (replication_stream_poll or handle) exit.
                    select! {
                        _ = replication_stream_poll(duplex_stream, tx.clone()) => {
                            panic!("replication_stream_poll exited, panic to restart")
                        }
                        _ = handle => {
                            panic!("start_forwarder exited, panic to restart")
                        }
                    }
                }
            })
        })
        .expect("Cannot create the Children for Bastion");
}
