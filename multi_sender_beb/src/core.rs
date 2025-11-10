use crate::aggregator::Aggregator;
use crate::consensus::{ConsensusMessage, Round};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::messages::{Block, DependentMeta, Proposal, ProposalType, Vote, VoteType, QC};
use crate::proposer::ProposerMessage;
use crate::timer::Timer;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, Hash as _};
use crypto::{PublicKey, SignatureService};
use log::{debug, error, info, warn};
use network::SimpleSender;
use std::collections::{HashMap, HashSet};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub struct Core {
    committee: Committee,
    committable_blocks: HashMap<Digest, Round>,
    leader_elector: LeaderElector,
    locked: QC,
    name: PublicKey,
    qc_sender: SimpleSender,
    round: Round,
    rx_proposer: Receiver<Proposal>,
    rx_message: Receiver<ConsensusMessage>,
    rx_synchronizer: Receiver<Block>,
    signature_service: SignatureService,
    store: Store,
    sync_requests: HashSet<Digest>,
    timer: Timer,
    tx_output: Sender<Block>,
    tx_proposer: Sender<ProposerMessage>,
    vote_sender: SimpleSender,
}

// Identifier of the Genesis round.
const GENESIS: u64 = 0;

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        leader_elector: LeaderElector,
        timeout_delay: u64,
        rx_message: Receiver<ConsensusMessage>,
        rx_proposer: Receiver<Proposal>,
        rx_synchronizer: Receiver<Block>,
        tx_proposer: Sender<ProposerMessage>,
        tx_output: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                committable_blocks: HashMap::new(),
                leader_elector,
                locked: QC::genesis(),
                name,
                qc_sender: SimpleSender::new(),
                round: 1,
                rx_proposer,
                rx_message,
                rx_synchronizer,
                signature_service,
                store,
                sync_requests: HashSet::new(),
                timer: Timer::new(timeout_delay),
                tx_output,
                tx_proposer,
                vote_sender: SimpleSender::new(),
            }
            .run()
            .await
        });
    }

    // Sends the given ConsensusMessage to all but self.
    async fn broadcast(&mut self, m: ConsensusMessage) {
        debug!("Broadcasting {:?}", m);
        let addresses = self.committee.others_consensus_sockets(&self.name);
        let message =
            bincode::serialize(&m).expect(format!("Failed to serialize message {:?}", m).as_str());
        let m_bytes = Bytes::from(message);

        match m {
            ConsensusMessage::Vote(_) => self.vote_sender.broadcast(addresses, m_bytes).await,
            _ => (),
        }
    }

    // Unicasts the given ConsensusMessage to the given recipient if recipient is not self.
    async fn send_to(&mut self, m: ConsensusMessage, recipient: &PublicKey) {
        debug!("Unicasting {:?}", m);

        if *recipient != self.name {
            let address = self
                .committee
                .consensus(recipient)
                .expect("Target node is not in the committee")
                .consensus_to_consensus;
            let message = bincode::serialize(&m)
                .expect(format!("Failed to serialize message {:?}", m).as_str());
            let m_bytes = Bytes::from(message);

            match m {
                ConsensusMessage::Vote(_) => self.vote_sender.send(address, m_bytes).await,
                _ => (),
            }
        }
    }

    async fn store_block(&mut self, block: &Block) {
        // Write to disk
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
        debug!("Stored block {:?}", block);
    }

    async fn cleanup(&mut self, r: Round) {
        // Remove Prepare messages and stop trying to send proposals for all prior rounds.
        self.cleanup_proposals(r).await;
    }

    async fn local_timeout_round(&mut self) -> ConsensusResult<()> {
        // Failed to form a QC this round.
        // warn!("Timeout reached for round {}", self.round);
        self.propose_if_leader().await;
        // Ensure that we trigger Timeout Sync for r at most once every timeout_delay.
        // self.timer.reset();
        Ok(())
    }

    async fn propose_if_leader(&mut self) {
        // info!("My id is {:?}", self.committee.id(&self.name));
        if self.committee.id() == 0 {
            self.tx_proposer
                .send(ProposerMessage::Propose())
                .await
                .expect("Failed to send message to proposer");
        }
    }

    async fn cleanup_proposals(&mut self, r: Round) {
        // Stop trying to deliver proposals for all rounds up to and including this round.
        // Invocation of this function upon entering a new round (i.e. upon QC or TC
        // observation) prevents Byzantine nodes from draining our resources by never
        // ACKing proposals.
        self.tx_proposer
            .send(ProposerMessage::Cleanup(r))
            .await
            .expect("Failed to send message to proposer");
    }

    async fn propose_normal(&mut self) {
        self.propose_if_leader().await
    }

    async fn handle_proposal(&mut self, p: Proposal) -> ConsensusResult<()> {
        // let r =  p.block.round;
        // *self.processing_blocks.entry(r).or_insert(0) += 1;
        // let counter = self.processing_blocks.get(&r).unwrap_or(&0);
        // if (counter == &5){
        info!(
            "Received {:?} counter {:?}",
            p,
            p.block.payload.last().unwrap()
        );
        // }

        Ok(())
    }

    async fn handle_dependent_meta(&mut self, meta: DependentMeta) -> ConsensusResult<()> {
        info!("Received Dependent Meta {:?}", meta.round);
        Ok(())
    }

    pub async fn run(&mut self) {
        // Upon booting, generate the very first block (if we are the leader).
        // Also, schedule a timer in case we don't hear from the leader.
        self.timer.reset();
        // self.propose_normal().await;

        // This is the main loop: it processes incoming blocks, votes and QCs,
        // and receives timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.rx_message.recv() =>
                match message {
                    ConsensusMessage::Propose(proposal) => self.handle_proposal(proposal).await,
                    ConsensusMessage::DependentMeta(meta) => self.handle_dependent_meta(meta).await,
                    _ => panic!("Unexpected protocol message")
                },
                Some(proposal) = self.rx_proposer.recv() => self.handle_proposal(proposal).await,
                () = &mut self.timer => self.local_timeout_round().await,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
