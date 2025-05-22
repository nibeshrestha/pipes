use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, Proposal, QC};
use bytes::Bytes;
use config::{Committee, Parameters};
use crypto::Hash;
use crypto::{PublicKey, SignatureService};
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use primary::Certificate;
use std::collections::HashMap;
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
    last_proposed: Block,
    max_block_delay: u64,
    max_block_size: usize,
    max_packet_size: usize,
    rx_mempool: Receiver<Certificate>,
    rx_core: Receiver<ProposerMessage>,
    tx_proposer_core: Sender<Proposal>,
    tx_committer: Sender<Certificate>,
    buffer: Vec<u8>,
    network: ReliableSender,
    proposal_request: Option<QC>,
    round: Round,
    counter: u64,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        consensus_only: bool,
        committee: Committee,
        max_block_size: usize,
        max_packet_size: usize,
        rx_mempool: Receiver<Certificate>,
        rx_core: Receiver<ProposerMessage>,
        tx_proposer_core: Sender<Proposal>,
        tx_committer: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                consensus_only,
                committee,
                in_progress: HashMap::new(),
                last_proposed: Block::genesis(),
                max_block_delay: 2_000,
                max_block_size,
                max_packet_size,
                rx_mempool,
                rx_core,
                tx_proposer_core,
                tx_committer,
                buffer: Vec::new(),
                network: ReliableSender::new(),
                proposal_request: None,
                round: 1,
                counter: 0
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
    fn get_payload(&mut self) -> Vec<u8> {
        
        if self.buffer.len() < self.max_block_size {
            self.buffer.drain(..).collect()
        } else {
            self.buffer.drain(0..self.max_block_size).collect()
        }
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
        // References to the connections that we are continuously trying to deliver
        // this proposal on. We keep them around to ensure that we keep sending until:
        //   1. we deliver it (indicated by an ACK from the recipient), or;
        //   2. we observe either a QC for it (indicating our job is done), or;
        //   3. we observe a TC for the round (indicating the network is asynchronous), or;
        //   4. we replace it with another proposal for this round (only occurs if Optimistic).
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

    async fn make_proposal(&mut self) -> Proposal {
        let mut payload;
        
        payload = vec![0u8; self.max_packet_size];

        let b = Block::new(
            self.name,
            payload,
            self.round,
            self.counter,
        )
        .await;
        // self.counter += 1;

        // if self.counter == (self.max_block_size/self.max_packet_size) as u64 {
        self.round += 1;
        //     self.counter = 0;
        // }

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
    }

    async fn run(&mut self) {

        loop {
            tokio::select! {
                Some(m) = self.rx_core.recv() => {
                    match m {
                        ProposerMessage::Propose() => self.propose().await,
                        ProposerMessage::Cleanup(r) => self.cleanup(r)
                    }
                },
            }
        }
    }
}
