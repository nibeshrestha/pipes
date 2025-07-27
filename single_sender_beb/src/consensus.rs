use crate::core::Core;
use crate::error::ConsensusError;
use crate::leader::LeaderElector;
use crate::messages::{Block, DependentMeta, Proposal, Vote, QC};
use crate::proposer::Proposer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters};
use crypto::{Digest, PublicKey, SignatureService};
use futures::SinkExt as _;
use log::{debug, info};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The default channel capacity for each channel of the consensus.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    Propose(Proposal),
    Vote(Vote),
    DependentMeta(DependentMeta),
    SyncRequest(Digest, PublicKey),
    SyncResponse(Block),
}

pub struct Consensus;

impl Consensus {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        tx_output: Sender<Block>,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        let (tx_consensus, rx_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_proposer_core, rx_proposer_core) = channel(CHANNEL_CAPACITY);
        let (tx_sync_core, rx_sync_core) = channel(CHANNEL_CAPACITY);
        let (tx_core_proposer, rx_core_proposer) = channel(CHANNEL_CAPACITY);
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);

        // Spawn the network receiver.
        let mut address = committee
            .consensus(&name)
            .expect("Our public key is not in the committee")
            .consensus_to_consensus;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            ConsensusReceiverHandler {
                tx_consensus,
                tx_helper,
            },
        );
        info!(
            "Node {} listening to consensus messages on {}",
            name, address
        );

        // Make the leader election module.
        let leader_elector = LeaderElector::new(committee.clone());
        let is_proposer = name == leader_elector.get_leader(1);

        // Spawn the consensus core.
        Core::spawn(
            name,
            committee.clone(),
            signature_service.clone(),
            store.clone(),
            leader_elector,
            parameters.timeout_delay,
            /* rx_message */ rx_consensus,
            rx_proposer_core,
            rx_sync_core,
            tx_core_proposer,
            tx_output,
        );

        // Spawn the block proposer.
        Proposer::spawn(
            name,
            parameters.consensus_only,
            committee.clone(),
            parameters.meta_indep_size,
            parameters.meta_dep_size,
            /* rx_message */ rx_core_proposer,
            tx_proposer_core,
            parameters.client_rate,
            is_proposer,
            parameters.alpha,
            parameters.bandwidth,
            parameters.nodes,
            parameters.effective_bandwidth
        );
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct ConsensusReceiverHandler {
    tx_consensus: Sender<ConsensusMessage>,
    tx_helper: Sender<(Digest, PublicKey)>,
}

#[async_trait]
impl MessageHandler for ConsensusReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(ConsensusError::SerializationError)? {
            ConsensusMessage::SyncRequest(missing, origin) => self
                .tx_helper
                .send((missing, origin))
                .await
                .expect("Failed to send consensus message"),
            // message @ ConsensusMessage::Propose(..) => {
            //     // TODO: Remove
            //     debug!("Acking Proposal: {:?}", message);
            //     // Reply with an ACK.
            //     let _ = writer.send(Bytes::from("Ack")).await;

            //     // Pass the message to the consensus core.
            //     self.tx_consensus
            //         .send(message)
            //         .await
            //         .expect("Failed to consensus message")
            // }
            message => {
                // debug!("Received message from peer: {:?}", message);
                self.tx_consensus
                    .send(message)
                    .await
                    .expect("Failed to consensus message")
            }
        }
        Ok(())
    }
}
