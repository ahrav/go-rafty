package raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type logEntry struct {
	term    int
	command any
}

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

// Node is a representation of a raft node.
type Node struct {
	mu sync.Mutex

	id      int
	peerIDs []int

	server *Server

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []logEntry

	// Volatile state on all servers.
	state              consensusState
	electionResetEvent time.Time
}

// NewNode returns a new raft node with the given id and peer ids. It also takes a ready channel
// which is used to signal when the node is connected to the network and ready to start its state machine.
func NewNode(id int, peerIDs []int, ready <-chan struct{}) *Node {
	n := &Node{
		id:       id,
		peerIDs:  peerIDs,
		state:    follower,
		votedFor: -1,
	}

	go n.run(ready)

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

	if n.votedFor == -1 || n.votedFor == args.CandidateID {
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

// startElection starts a new election with this node as a candidate.
func (n *Node) startElection() {
	n.state = candidate
	n.currentTerm++
	n.votedFor = n.id
	n.electionResetEvent = time.Now()
	fmt.Printf("node %v starting election in term %v\n", n.id, n.currentTerm)

	// Send RequestVote RPCs to all other nodes.
	var votesReceived atomic.Int32
	votesReceived.Add(1) // Vote for self.

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
				votesReceived.Add(1)
				if int(votesReceived.Load())*2 > len(n.peerIDs)+1 {
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
	Term         int
	LeaderID     int
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
		reply.Success = true
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
		args := AEArgs{Term: currentTerm, LeaderID: n.id}
		go func(peerID int) {
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
		}(peerID)
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
