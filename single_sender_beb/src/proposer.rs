use crate::consensus::{ConsensusMessage, Round};
use crate::error::ConsensusResult;
use crate::messages::{Block, DependentMeta, Proposal, QC};
use crate::timer::Timer;
use bytes::Bytes;
use config::{Committee, Parameters};
use crypto::Hash;
use crypto::{PublicKey, SignatureService};
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use std::collections::HashMap;
use std::convert::TryInto;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[derive(Debug)]
pub enum ProposerMessage {
    Propose(),
    Cleanup(Round),
}

pub struct Proposer {
    name: PublicKey,
    consensus_only: bool,
    committee: Committee,
    in_progress: HashMap<Round, Vec<CancelHandler>>,
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>,
    last_proposed: Block,
    payload_size: usize,
    effective_payload_size: usize,
    rx_core: Receiver<ProposerMessage>,
    tx_proposer_core: Sender<Proposal>,
    buffer: Vec<u64>,
    network: ReliableSender,
    proposal_request: Option<QC>,
    round: Round,
    counter: u64,
    meta_indep_size: usize,
    meta_dep_size: usize,
    alpha: f32,
    bandwidth: u64,
    timer: Timer,
    is_proposer: bool,

    /// false implies block proposed, true implies metadata sent
    last_action: bool,
    nodes: u64,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        consensus_only: bool,
        committee: Committee,
        meta_indep_size: usize,
        meta_dep_size: usize,
        rx_core: Receiver<ProposerMessage>,
        tx_proposer_core: Sender<Proposal>,
        client_rate: u64,
        is_proposer: bool,
        alpha: f32,
        bandwidth: u64,
        nodes: u64,
        effective_bandwidth: f32,
    ) {
        tokio::spawn(async move {
            let meta_size = meta_indep_size + meta_dep_size;
            let payload_size: usize = ((alpha * meta_size as f32) / (1 as f32 - alpha)) as usize;
            let effective_payload_size = (payload_size as f32 * effective_bandwidth) as usize;
            Self {
                name,
                consensus_only,
                committee,
                in_progress: HashMap::new(),
                cancel_handlers: HashMap::new(),
                last_proposed: Block::genesis(),
                payload_size: payload_size,
                effective_payload_size,
                rx_core,
                tx_proposer_core,
                buffer: Vec::new(),
                network: ReliableSender::new(),
                proposal_request: None,
                round: 1,
                counter: 0,
                meta_indep_size,
                meta_dep_size,
                alpha,
                bandwidth,
                timer: Timer::new(client_rate),
                is_proposer,
                last_action: false,
                nodes,
            }
            .run()
            .await;
        });
    }

    // TODO: This function simulates payload creation. An actual payload manager will need to
    // have logic for identifying "pending" txs in order to prevent duplicates and/or lost txs.
    // Such pending txs should be those included in blocks that have been proposed/voted on
    // but have not yet satisfied the commit rule. Txs should only be removed from the Proposer
    // once they have been committed.
    fn get_payload(&mut self) -> u64 {
        if self.buffer.len() > 0 {
            self.buffer.remove(0)
        } else {
            info!("Empty buffer");
            0u64
        }
    }

    async fn client_reset(&mut self) {
        self.buffer.push(self.counter);

        let mut propagation_time;

        // if self.last_action {
        //     propagation_time =
        //         (self.meta_dep_size as u64 * (self.nodes - 1) * 1000) / self.bandwidth;
        //     info!("Received sample txn {:?}", self.round + 1);
        //     self.send_dependent_meta().await;
        //     info!(
        //         "Sent dep meta {:?} propogation time {:?}",
        //         self.round, propagation_time
        //     );
        // } else {
            propagation_time =
                // ((self.payload_size + self.meta_indep_size) as u64 * (self.nodes - 1) * 1000)
                //     / self.bandwidth;
                ((self.payload_size ) as u64 * (self.nodes - 1) * 1000)
                    / self.bandwidth;
            self.propose().await;
            info!("propogation time {:?}", propagation_time);
        // }

        self.last_action = !self.last_action;
        // self.timer.reset();
        self.timer.set_timer(1000);
    }

    async fn send_proposal(&mut self, proposal: Proposal) {
        info!("Created {:?}", proposal);
        let (names, addresses): (Vec<_>, _) = self
            .committee
            .others_consensus(&self.name)
            .into_iter()
            .map(|(name, x)| (name, x.consensus_to_consensus))
            .unzip();

        debug!(
            "Sending to {:?} {:?}. Self is: {}",
            names, addresses, self.name
        );

        let message = bincode::serialize(&ConsensusMessage::Propose(proposal))
            .expect("Failed to serialize block");

        let handles = self
            .network
            .broadcast(addresses, Bytes::from(message))
            .await;
        self.in_progress.insert(self.last_proposed.round, handles);
    }

    fn record_proposal(&mut self, b: Block) {
        if b.digest() != self.last_proposed.digest() {
            info!("Created {:?}", b);
            // Record the most recent proposal to ensure that the same payload is used for
            // any additional proposals created for this round, and that the block creation
            // log only prints once.
            self.last_proposed = b;
        }
    }

    async fn send_dependent_meta(&mut self) {
        let mut meta = vec![0u8; self.meta_dep_size];

        let m = DependentMeta::new(self.name, meta, self.round).await;

        let (names, addresses): (Vec<_>, _) = self
            .committee
            .others_consensus(&self.name)
            .into_iter()
            .map(|(name, x)| (name, x.consensus_to_consensus))
            .unzip();

        let message = bincode::serialize(&ConsensusMessage::DependentMeta(m))
            .expect("Failed to serialize block");

        let handles = self
            .network
            .broadcast(addresses, Bytes::from(message))
            .await;
        self.cancel_handlers.insert(self.round, handles);
    }

    async fn make_proposal(&mut self) -> Proposal {
        let mut payload;

        payload = vec![0u8; self.payload_size - 8];
        let mut meta_indep = vec![0u8; 0];
                // let mut meta_indep = vec![0u8; self.meta_indep_size];


        self.round += 1;
        let sample_tx: u64 = self.round;

        let b = Block::new(
            self.name,
            sample_tx,
            payload,
            meta_indep,
            self.round,
            self.counter,
        )
        .await;

        // self.record_proposal(b.clone());
        Proposal::new(b)
    }

    async fn propose(&mut self) {
        // Generate a new Proposal.
        let proposal = self.make_proposal().await;
        // Send the Proposal to the Core for local processing.
        self.tx_proposer_core
            .send(proposal.clone())
            .await
            .expect("Failed to send block");
        // Broadcast the Proposal.
        self.send_proposal(proposal).await;
    }

    fn cleanup(&mut self, r: Round) {
        // Core sent a Cleanup request after we transitioned to a new round.
        // Stop trying to deliver proposals for previous rounds. Ensures
        // we are able to use the resend loop to reliably deliver our proposals
        // to honest validators while preventing Byzantine validators from arbitrarily
        // consuming our bandwidth by never ACKing.
        self.in_progress
            .retain(|proposal_round, _| *proposal_round > r);
        self.cancel_handlers.retain(|round, _| *round > r);
    }

    async fn run(&mut self) {
        self.timer.set_timer(1000);

        loop {
            tokio::select! {
                Some(m) = self.rx_core.recv() => {
                    match m {
                        // ProposerMessage::Propose() => self.propose().await,
                        ProposerMessage::Cleanup(r) => self.cleanup(r),
                        _ => ()
                    }
                },
                () = &mut self.timer => {
                    if self.is_proposer {
                        self.client_reset().await
                    }
                },
            }
        }
    }
}
