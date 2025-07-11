use crate::consensus::Round;
use crate::error::{ConsensusError, ConsensusResult};
use config::Committee;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::Certificate;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt;

#[cfg(test)]
#[path = "tests/messages_tests.rs"]
pub mod messages_tests;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub author: PublicKey,
    // Digest::default signifies a dummy block, unless round is also
    // 0, in which case the block is the Genesis block.
    pub parent: Digest,
    pub sample_tx: u64,
    pub payload: Vec<u8>,
    pub meta_indep: Vec<u8>,
    pub meta_dep: Vec<u8>,
    // Height in Simplex
    pub round: Round,
    pub idx: u64,
}

impl Block {
    pub async fn new(
        author: PublicKey,
        sample_tx: u64,
        payload: Vec<u8>,
        meta_indep: Vec<u8>,
        meta_dep: Vec<u8>,
        round: Round,
        idx: u64,
    ) -> Self {
        let mut b = Block {
            author,
            parent: Digest::default(),
            sample_tx,
            payload,
            meta_indep,
            meta_dep,
            round,
            idx,
        };
        b
    }

    pub fn dummy(round: Round) -> Self {
        Self {
            author: PublicKey::default(),
            parent: Digest::default(),
            sample_tx: 0 as u64,
            payload: Vec::default(),
            meta_indep: Vec::default(),
            meta_dep: Vec::default(),
            round,
            idx: 0 as u64,
        }
    }

    pub fn genesis() -> Self {
        Self {
            author: PublicKey::default(),
            parent: Digest::default(),
            sample_tx: 0,
            payload: Vec::default(),
            meta_indep: Vec::default(),
            meta_dep: Vec::default(),
            round: 0,
            idx: 0,
        }
    }

    pub fn is_well_formed(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ignore Genesis block.
        if self.digest() != Block::genesis().digest() {
            // Ensure the proposer has voting rights.
            let voting_rights = committee.stake(&self.author);
            ensure!(
                voting_rights > 0,
                ConsensusError::UnknownAuthority(self.author)
            );
            // Ensure the included signature is that of the author.
            // self.signature.verify(&self.digest(), &self.author)?;
        }
        Ok(())
    }
}

impl Hash for Block {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.sample_tx.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: CMB(author {}, sample {}, payload_len {} meta_indep {} meta_dep {})",
            self.digest(),
            self.author,
            self.sample_tx,
            self.payload.len() + 8,
            self.meta_indep.len(),
            self.meta_dep.len()
        )
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "CMB{}", self.round)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Proposal {
    pub block: Block,
}

impl Proposal {
    pub fn new(block: Block) -> Self {
        Self { block }
    }

    pub fn is_well_formed(&self, committee: &Committee) -> ConsensusResult<()> {
        self.block.is_well_formed(committee)?;

        Ok(())
    }
}

impl Hash for Proposal {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.block.digest());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Proposal {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Block {:?})", self.block,)
    }
}

impl fmt::Display for Proposal {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Proposal B{}", self.block.round)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum ProposalType {
    Fallback,
    Normal,
    Optimistic,
}

impl Hash for ProposalType {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        match self {
            Self::Fallback => hasher.update("Fallback"),
            Self::Normal => hasher.update("Normal"),
            Self::Optimistic => hasher.update("Optimistic"),
        }
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Display for ProposalType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::Fallback => write!(f, "Fallback"),
            Self::Normal => write!(f, "Normal"),
            Self::Optimistic => write!(f, "Optimistic"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum VoteType {
    Commit,
    Dummy,
    Prepare,
}

impl Hash for VoteType {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        match self {
            Self::Commit => hasher.update("C"),
            Self::Dummy => hasher.update("D"),
            Self::Prepare => hasher.update("P"),
        }
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Display for VoteType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::Commit => write!(f, "C"),
            Self::Dummy => write!(f, "D"),
            Self::Prepare => write!(f, "P"),
        }
    }
}

// TODO: Timeouts and Prepares should come with justification to prevent
// Byzantine nodes spamming messages for higher rounds.
#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub author: PublicKey,
    pub hash: Digest,
    pub kind: VoteType,
    pub round: Round,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        author: PublicKey,
        hash: Digest,
        kind: VoteType,
        round: Round,
        mut signature_service: SignatureService,
    ) -> Self {
        let vote = Self {
            author,
            hash: hash.clone(),
            kind,
            round,
            signature: Signature::default(),
        };
        // Only sign the block. The network channels are already authenticated so
        // no need to sign the whole message.
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn dummy(round: Round) -> Self {
        let dummy_block = Block::dummy(round);
        Self {
            author: PublicKey::default(),
            hash: dummy_block.digest(),
            kind: VoteType::Prepare,
            round,
            signature: Signature::default(),
        }
    }

    pub fn is_well_formed(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );
        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.hash);
        hasher.update(self.kind.digest());
        hasher.update(self.round.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "V({}, {}, {}, {})",
            self.kind, self.author, self.round, self.hash
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QC {
    pub hash: Digest,
    pub kind: VoteType,
    pub round: Round,
    pub votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn genesis() -> Self {
        QC {
            hash: Block::genesis().digest(),
            kind: VoteType::Commit,
            round: 0,
            votes: Vec::new(),
        }
    }

    pub fn is_dummy(&self) -> bool {
        self.kind == VoteType::Dummy
    }

    pub fn is_well_formed(&self, committee: &Committee) -> ConsensusResult<()> {
        if self.round == 0 {
            Ok(())
        } else {
            // Ensure the QC has a quorum.
            let mut weight = 0;
            let mut used = HashSet::new();
            for (name, _) in self.votes.iter() {
                ensure!(!used.contains(name), ConsensusError::AuthorityReuse(*name));
                let voting_rights = committee.stake(name);
                ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
                used.insert(*name);
                weight += voting_rights;
            }
            // TODO: Change to error log instead of panic.
            ensure!(
                weight >= committee.quorum_threshold(),
                ConsensusError::QCRequiresQuorum(self.round)
            );

            // Check the signatures.
            Signature::verify_batch(&self.digest(), &self.votes).map_err(ConsensusError::from)
        }
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.hash);
        hasher.update(&self.kind.digest());
        hasher.update(self.round.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self.kind {
            VoteType::Commit => write!(f, "CommitQC({}, {})", self.hash, self.round),
            _ => write!(f, "PrepareQC({}, {}, {})", self.kind, self.hash, self.round),
        }
    }
}

impl PartialEq for QC {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.hash == other.hash && self.round == other.round
    }
}
