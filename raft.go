package raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type consensusState int

const (
	follower consensusState = iota
	candidate
	leader
	dead
)

func (cs consensusState) String() string {
	switch cs {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	case dead:
		return "dead"
	default:
		return "unknown"
	}
}

type logEntry struct {
	term    int
	command any
}

type commitEntry struct {
	index   int // Index of log entry to commit.
	command any // Command from client to commit.
	term    int // Term of leader which created entry at which a client command is committed.
}

// Node is a representation of a raft node.
type Node struct {
	mu sync.Mutex

	id      int
	peerIDs []int

	server *Server

	// channel where the node sends committed entries to the client.
	commitChan chan<- commitEntry

	// notifications channel for when a new commit is ready to be sent on the commitChan.
	newCommitReadyChan chan struct{}

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []logEntry

	// Volatile state on all servers.
	state              consensusState
	electionResetEvent time.Time
	commitIndex        int
	lastApplied        int

	// Volatile state on leaders.
	nextIndex  map[int]int // For each server, index of the next log entry to send to that server.
	matchIndex map[int]int // For each server, index of highest log entry known to be replicated on server.
}

// NewNode returns a new raft node with the given id and peer ids. It also takes a ready channel
// which is used to signal when the node is connected to the network and ready to start its state machine.
func NewNode(id int, peerIDs []int, server *Server, ready <-chan struct{}, commitChan chan<- commitEntry) *Node {
	n := &Node{
		id:                 id,
		peerIDs:            peerIDs,
		server:             server,
		state:              follower,
		votedFor:           -1,
		commitChan:         commitChan,
		newCommitReadyChan: make(chan struct{}, 16),
		commitIndex:        -1,
		lastApplied:        -1,
		nextIndex:          make(map[int]int),
		matchIndex:         make(map[int]int),
	}

	go n.run(ready)
	go n.commitChanDispatcher()

	return n
}

func (n *Node) run(ready <-chan struct{}) {
	// The node is dormant until it is signalled to be ready.
	<-ready
	n.mu.Lock()
	n.electionResetEvent = time.Now()
	n.mu.Unlock()
	n.runElectionTimer() // Begin countdown for leader election.
}

func (n *Node) runElectionTimer() {
	timeout := electionTimeout()
	n.mu.Lock()
	startTerm := n.currentTerm
	n.mu.Unlock()
	fmt.Printf("election timer started (%v) for node %v in term %v\n", timeout, n.id, startTerm)

	// Start a timer for election timeout. It should run in a separate goroutine for the node's lifetime.
	// It can fire for 2 reasons:
	// 1. Election timeout: If the node doesn't receive any heartbeat or appendEntries RPCs from the leader before the timeout.
	// 2. Election won: Timer no longer needed as the node has become the leader.
	const electionTimerInterval = 10 * time.Millisecond
	ticker := time.NewTicker(electionTimerInterval)
	defer ticker.Stop()
	for range ticker.C {
		if n.shouldStopElectionTimer(timeout) {
			return
		}
	}
}

// electionTimeout returns a semi-random election timeout duration.
// Note the use of randomization to try prevent simultaneous candidates.
func electionTimeout() time.Duration { return time.Duration(150+rand.Intn(150)) * time.Millisecond }

func (n *Node) shouldStopElectionTimer(timeout time.Duration) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != candidate && n.state != follower {
		fmt.Printf("election timer stopped for node %v in term %v\n", n.id, n.currentTerm)
		return true
	}

	if time.Since(n.electionResetEvent) >= timeout {
		fmt.Printf("election timer timeout reached, starting election for node %v in term %v\n", n.id, n.currentTerm)
		n.startElection()
		return true
	}

	return false
}

// commitChanDispatcher sends committed entries to the commitChan. It uses the newCommitReadyChan to be notified
// when a new commit is ready to be sent. It then calculates all the commits up to the latest index and sends them
// on the commitChan.
func (n *Node) commitChanDispatcher() {
	for range n.newCommitReadyChan {
		n.mu.Lock()
		savedTerm := n.currentTerm
		savedLastApplied := n.lastApplied
		var entries []logEntry
		if n.commitIndex > n.lastApplied {
			// All new entries since the last applied entry.
			entries = n.log[n.lastApplied+1 : n.commitIndex+1]
		}
		n.mu.Unlock()
		fmt.Printf("commitChanDispatcher for node %v in term %v, entries %v, saveLastApplied %v\n", n.id, savedTerm, entries, savedLastApplied)

		for i, entry := range entries {
			n.commitChan <- commitEntry{index: n.lastApplied + i + 1, command: entry.command, term: entry.term}
		}
	}
}

// Submit submits a new command to the raft node. Clients use the commit channel passed into the constructor
// of NewNode to be notified when the command is committed. This function returns true iff this node is the leader.
// If it's not the leader, the client should retry the command on the new leader.
func (n *Node) Submit(command any) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != leader {
		return false
	}

	n.log = append(n.log, logEntry{term: n.currentTerm, command: command})
	fmt.Printf("node %v received client command %v, appending to log as entry %v\n", n.id, command, len(n.log)-1)

	return true
}

// Stop stops the node.
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = dead
	fmt.Printf("node %v stopping, now dead\n", n.id)
}

type RVArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RVResult struct {
	Term        int
	VoteGranted bool
}

// RequestVote is called by candidates to gather votes.
func (n *Node) RequestVote(args *RVArgs, reply *RVResult) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == dead {
		fmt.Printf("node %v rejecting RequestVote RPC from node %v, it's dead\n", n.id, args.CandidateID)
		return nil
	}

	fmt.Printf("node %v received RequestVote RPC from node %v\n", n.id, args.CandidateID)

	if args.Term > n.currentTerm {
		fmt.Printf("node %v stepping down because of higher term %v\n", n.id, args.Term)
		n.becomeFollower(args.Term)
	}

	if args.Term < n.currentTerm {
		fmt.Printf("node %v rejecting RequestVote RPC from node %v, because of lower term %v\n", n.id, args.CandidateID, args.Term)
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return nil
	}

	lastLogIndex, lastLogTerm := n.lastLogIndexAndTerm()
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		n.votedFor = args.CandidateID
		reply.Term = n.currentTerm
		reply.VoteGranted = true
		n.electionResetEvent = time.Now()
		return nil
	}

	fmt.Printf("node %v rejecting RequestVote RPC from node %v, already voted for node %v\n", n.id, args.CandidateID, n.votedFor)
	reply.Term = n.currentTerm
	reply.VoteGranted = false
	return nil
}

// lastLogIndexAndTerm returns the index and term of the last log entry.
func (n *Node) lastLogIndexAndTerm() (int, int) {
	if len(n.log) == 0 {
		return -1, -1
	}
	idx := len(n.log) - 1
	return idx, n.log[idx].term
}

// startElection starts a new election with this node as a candidate.
func (n *Node) startElection() {
	n.state = candidate
	n.currentTerm++
	n.votedFor = n.id
	n.electionResetEvent = time.Now()
	fmt.Printf("node %v starting election in term %v\n", n.id, n.currentTerm)

	// Send RequestVote RPCs to all other nodes.
	votesReceived := 1 // Vote for self.

	for _, peerID := range n.peerIDs {
		go func(peerID int) {
			args := RVArgs{Term: n.currentTerm, CandidateID: n.id}

			var reply RVResult
			// Send RPC.
			if err := n.server.Call(peerID, "Node.RequestVote", &args, &reply); err != nil {
				// Not sure about this...
				fmt.Printf("node %v error sending RequestVote RPC to %v: %v\n", n.id, peerID, err)
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != candidate {
				fmt.Printf("node %v aborting election in term %v\n", n.id, n.currentTerm)
				return
			}

			if reply.Term > n.currentTerm {
				fmt.Printf("node %v stepping down because of higher term %v\n", n.id, reply.Term)
				n.becomeFollower(reply.Term)
				return
			}

			if reply.Term == n.currentTerm && reply.VoteGranted {
				votesReceived++
				if votesReceived*2 > len(n.peerIDs)+1 {
					fmt.Printf("node %v wins election in term %v\n", n.id, n.currentTerm)
					n.becomeLeader()
					return
				}
			}
		}(peerID)
	}

	// Start election timer for next election if the election fails.
	go n.runElectionTimer()
}

func (n *Node) becomeFollower(term int) {
	fmt.Printf("node %v becoming follower in term %v\n", n.id, term)
	n.state = follower
	n.currentTerm = term
	n.votedFor = -1
	n.electionResetEvent = time.Now()

	go n.runElectionTimer()
}

func (n *Node) becomeLeader() {
	fmt.Printf("node %v becoming leader in term %v\n", n.id, n.currentTerm)
	n.state = leader

	go func() {
		const heartbeatInterval = 50 * time.Millisecond
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for range ticker.C {
			n.sendHeartbeats()

			n.mu.Lock()
			if n.state != leader {
				fmt.Printf("stopping heartbeat, no longer leader in term %v for node %v\n", n.currentTerm, n.id)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
		}
	}()
}

type AEArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AEResult struct {
	Term    int
	Success bool
}

// AppendEntries is called by the leader to replicate log entries and provide a form of heartbeat.
func (n *Node) AppendEntries(args *AEArgs, reply *AEResult) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == dead {
		fmt.Printf("node %v rejecting AppendEntries RPC from node %v, it's dead\n", n.id, args.LeaderID)
		return nil
	}

	fmt.Printf("node %v received AppendEntries RPC from node %v\n", n.id, args.LeaderID)

	if args.Term > n.currentTerm {
		fmt.Printf("node %v stepping down because of higher term %v\n", n.id, args.Term)
		n.becomeFollower(args.Term)
	}

	reply.Term = n.currentTerm
	reply.Success = false

	if args.Term < n.currentTerm {
		fmt.Printf("node %v rejecting AppendEntries RPC from node %v, because of lower term %v\n", n.id, args.LeaderID, args.Term)
		return nil
	}

	if n.state != follower {
		fmt.Printf("node %v becoming follower in term %v\n", n.id, args.Term)
		n.becomeFollower(args.Term)
		n.electionResetEvent = time.Now()

		// Check if our log contains an entry at PrevLogIndex whose term matches PrevLogTerm.
		if args.PrevLogIndex == -1 || (args.PrevLogIndex < len(n.log) && n.log[args.PrevLogIndex].term == args.PrevLogTerm) {
			reply.Success = true

			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for logInsertIndex < len(n.log) && newEntriesIndex < len(args.Entries) {
				if n.log[logInsertIndex].term != args.Entries[newEntriesIndex].term {
					n.log = n.log[:logInsertIndex]
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			// Append any new entries not already in the log.
			if newEntriesIndex < len(args.Entries) {
				n.log = append(n.log, args.Entries[newEntriesIndex:]...)
			}

			// Set commitIndex to the minimum of leaderCommit and the index of the last new entry.
			if args.LeaderCommit > n.commitIndex {
				n.commitIndex = min(args.LeaderCommit, len(n.log)-1)
				fmt.Printf("node %v updated commitIndex to %v\n", n.id, n.commitIndex)
				n.newCommitReadyChan <- struct{}{}
			}
		}
	}
	return nil
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.state != leader {
		fmt.Printf("no longer leader in term %v for node %v\n", n.currentTerm, n.id)
		n.mu.Unlock()
		return
	}
	currentTerm := n.currentTerm
	n.mu.Unlock()

	// Send empty AppendEntries RPCs as heartbeats to all other nodes.
	for _, peerID := range n.peerIDs {
		go func(peerID int) {
			n.mu.Lock()
			nextIndex := n.nextIndex[peerID]
			prevLogIndex := nextIndex - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = n.log[prevLogIndex].term
			}
			entries := n.log[nextIndex:]

			args := AEArgs{
				Term:         currentTerm,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.commitIndex,
			}
			n.mu.Unlock()
			fmt.Printf("node %v sending AppendEntries RPC to node %v\n", n.id, peerID)

			var reply AEResult
			// Send RPC.
			if err := n.server.Call(peerID, "Node.AppendEntries", &args, &reply); err != nil {
				// Not sure about this...
				fmt.Printf("node %v error sending AppendEntries RPC to %v: %v\n", n.id, peerID, err)
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > currentTerm {
				fmt.Printf("node %v stepping down because of higher term %v\n", n.id, reply.Term)
				n.becomeFollower(reply.Term)
				return
			}

			if n.state == leader && n.currentTerm == reply.Term {
				if reply.Success {
					n.nextIndex[peerID] = nextIndex + len(entries)
					n.matchIndex[peerID] = n.nextIndex[peerID] - 1
					fmt.Printf("node %v updated nextIndex to %v and matchIndex to %v for node %v\n", n.id, n.nextIndex[peerID], n.matchIndex[peerID], peerID)
					n.updateCommitIndex()
				} else {
					n.nextIndex[peerID]--
				}
			}
		}(peerID)
	}
}

func (n *Node) updateCommitIndex() {
	// Update commitIndex to the highest index replicated on a majority of servers.
	matchIndexes := make([]int, 0, len(n.matchIndex))
	for _, matchIndex := range n.matchIndex {
		matchIndexes = append(matchIndexes, matchIndex)
	}
	sort.Ints(matchIndexes)
	N := matchIndexes[len(matchIndexes)/2]
	if N > n.commitIndex && n.log[N].term == n.currentTerm {
		n.commitIndex = N
		fmt.Printf("node %v updated commitIndex to %v\n", n.id, n.commitIndex)
		n.newCommitReadyChan <- struct{}{}
	}
}

type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	node     *Node
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	peerClients map[int]*rpc.Client

	ready <-chan struct{}
	quit  chan any
	wg    sync.WaitGroup
}

// RPCProxy is a trivial pass-thru proxy type for ConsensusModule's RPC methods.
// It's useful for:
//   - Simulating a small delay in RPC transmission.
//   - Avoiding running into https://github.com/golang/go/issues/19957
//   - Simulating possible unreliable connections by delaying some messages
//     significantly and dropping others when RAFT_UNRELIABLE_RPC is set.
type RPCProxy struct {
	node *Node
}

func (s *Server) Call(id int, serviceMethod string, args any, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	// If this is called after shutdown (where client.Close is called), it will
	// return an error.
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}
