use crate::consensus::Round;
use crate::error::ConsensusResult;
use crate::messages::{Vote, VoteType, QC};
use config::{Committee, Stake};
use crypto::{Digest, PublicKey, Signature};
use log::debug;
use std::collections::{HashMap, HashSet};

#[cfg(test)]
#[path = "tests/aggregator_tests.rs"]
pub mod aggregator_tests;

pub struct Aggregator {
    committee: Committee,
    // Proposals indexed by round and block digest.
    votes_aggregators: HashMap<Round, HashMap<Digest, Box<QCMaker>>>,
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            votes_aggregators: HashMap::new(),
        }
    }

    pub fn add_vote(&mut self, vote: Vote) -> ConsensusResult<Option<QC>> {
        // TODO [issue #7]: A bad node may make us run out of memory by sending many votes
        // with different round numbers or different digests.

        // Add the new vote to our aggregator and see if we have a QC.
        // We create one aggregator for each block, which handles the different types
        // of Votes internally.
        self.votes_aggregators
            .entry(vote.round)
            .or_insert_with(HashMap::new)
            .entry(vote.hash.clone())
            .or_insert_with(|| Box::new(QCMaker::new()))
            .append(vote, &self.committee)
    }

    pub fn cleanup_prepares(&mut self, r: &Round) {
        self.votes_aggregators
            .retain(|block_round, _| block_round > r);
    }
}

struct QCMaker {
    // Dummy blocks should never receive Commit votes.
    received_commit: HashSet<PublicKey>,
    received: HashSet<PublicKey>,
    votes_commit: Vec<(PublicKey, Signature)>,
    votes: Vec<(PublicKey, Signature)>,
    weight_commit: Stake,
    weight: Stake,
}

impl QCMaker {
    pub fn new() -> Self {
        Self {
            received_commit: HashSet::new(),
            received: HashSet::new(),
            votes_commit: Vec::new(),
            votes: Vec::new(),
            weight_commit: 0,
            weight: 0,
        }
    }

    fn is_valid(&self, vote: &Vote) -> bool {
        let invalid = match vote.kind {
            // Honest send only once.
            VoteType::Commit => self.received_commit.contains(&vote.author),
            VoteType::Prepare => self.received.contains(&vote.author),
            VoteType::Dummy => {
                self.received.contains(&vote.author) && self.received_commit.len() != 0
            }
        };

        if invalid {
            debug!("Received duplicate vote from {}", vote.author);
        }

        !invalid
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> ConsensusResult<Option<QC>> {
        let author = vote.author;

        if self.is_valid(&vote) {
            // Verify the signature and voting rights before storing to prevent DoS
            // by unauthorised nodes. Verification is done after membership check on
            // self.used to prevent authorised but Byzantine nodes from draining compute
            // by sending duplicate Votes (HashMap membership checks are cheap, sig
            // verification is more expensive).
            vote.is_well_formed(committee)?;

            // Ensure we ignore duplicates and detect when Byzantine nodes send both Normal
            // and Fallback votes.
            let (votes, received, weight) = match vote.kind {
                VoteType::Commit => (
                    &mut self.votes_commit,
                    &mut self.received_commit,
                    &mut self.weight_commit,
                ),
                // Honest send either Fallback or Normal, and only once.
                VoteType::Dummy | VoteType::Prepare => {
                    (&mut self.votes, &mut self.received, &mut self.weight)
                }
            };

            received.insert(author);
            votes.push((author, vote.signature));

            *weight += committee.stake(&author);
            if *weight >= committee.quorum_threshold() {
                *weight = 0; // Ensures QC is only made once.
                return Ok(Some(QC {
                    hash: vote.hash.clone(),
                    kind: vote.kind,
                    round: vote.round,
                    votes: votes.clone(),
                }));
            }
        }

        Ok(None)
    }
}
