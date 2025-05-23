use crate::consensus::Round;
use config::Committee;
use crypto::PublicKey;
use std::{collections::{HashMap, VecDeque}, net::SocketAddr};

pub type LeaderElector = DeterministicFairSuccessionLeaderElector;

pub struct DeterministicFairSuccessionLeaderElector {
    // nodes_ids: Vec<PublicKey>,
    // schedule: Vec<usize>,
    addresses: Vec<(SocketAddr, PublicKey)>
}

/// Leader elector that ensures:
///   1. Every node leads the same number of rounds during the schedule.
///   2. Every node is succeeded by every other node during the schedule.
///   3. Every unique pair of nodes appears exactly once in the schedule.
impl DeterministicFairSuccessionLeaderElector {
    pub fn new(committee: Committee) -> Self {
        let n = committee.size();
        // Currently only support a static validator set, so can set this during construction.
        // let mut nodes_ids: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        // let schedule = Self::generate_schedule(n);
        let mut addresses: Vec<(SocketAddr, PublicKey)> = committee.authorities.into_iter().map(|(p, x)| {(x.consensus.consensus_to_consensus,p)}).collect();
        
        addresses.sort_by_key(|x| x.0);
    
        Self {
            addresses,
            // schedule,
        }
    }

    fn generate_schedule(n: usize) -> Vec<usize> {
        let mut successors: HashMap<usize, VecDeque<usize>> = HashMap::new();

        if n < 1 {
            return Vec::new();
        }

        // Generate the schedule of unique successors for each leader.
        // E.g, given n = 4:
        //   0: 1, 2, 3
        //   1: 2, 3, 0
        //   2: 3, 0, 1
        //   3: 0, 1, 2
        for i in 0..n {
            let mut index_schedule = VecDeque::new();

            for j in 0..n {
                let next_successor = (i + j) % n;

                if next_successor != i {
                    index_schedule.push_back(next_successor);
                }
            }

            successors.insert(i, index_schedule);
        }

        let mut schedule = Vec::new();
        let mut leader = 0;

        // Merge the schedules for each leader into a single schedule
        // that ensures that each leader is succeeded by every other
        // leader exactly once with no repetitions.
        //
        // E.g, given n = 4:
        // 0 1 2 3 0 2 0 3 1 3 2 1 0
        loop {
            schedule.push(leader);
            let leader_successors = successors.get_mut(&leader).unwrap();

            if let Some(successor) = leader_successors.pop_front() {
                leader = successor;
            } else {
                break;
            }
        }

        // Schedule should loop back around.
        assert!(schedule.first() == schedule.last());
        // Remove the duplicate, since we'll be looping over this array.
        schedule.pop();

        schedule
    }

    pub fn get_leader(&self, round: Round) -> PublicKey {
        let index = round as usize % self.addresses.len();
        // let leader = self.nodes_ids[index as usize];
        self.addresses[index].1
    }
}

/// Test helper.
#[cfg(test)]
pub fn test_generate_schedule_fairness(n: usize) {
    let schedule = DeterministicFairSuccessionLeaderElector::generate_schedule(n);

    // Each leader should be paired with ever other exactly once.
    assert!(schedule.len() == n * (n - 1));

    // Each leader should appear the same number of times in the schedule.
    for i in 0..n {
        assert!(schedule.iter().filter(|x| **x == i).count() == n - 1)
    }

    for start in 0..schedule.len() {
        // Each node should wait no longer than this before becoming the leader again.
        // Haven't managed to work out why this formula holds, but the wait is at most
        // this for n between 1 and 200. Not sure if it holds for all n.
        let end = start + 3 * n - 2;

        if end > schedule.len() {
            // Ignore the last n for now.
            break;
        }

        let cycle = &schedule[start..end];
        assert!(cycle.iter().filter(|x| **x == cycle[0]).count() >= 2);
    }
}

#[tokio::test]
async fn repeatedly_test_generate_schedule_fairness() {
    // We're currently only testing networks of up to 200 nodes, so don't need to
    // check for larger n. This is useful because the current method for checking
    // the max wait between elections of the same node is very expensive.
    for i in 1..200 {
        test_generate_schedule_fairness(i);
    }
}
