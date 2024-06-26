package chord

import (
	"math"
	"time"

	"github.com/bufrr/net/node"
	"github.com/bufrr/net/overlay/routing"
)

const (
	// RelayRoutingNumWorkers determines how many concurrent goroutines are
	// handling relay messages
	RelayRoutingNumWorkers = 1
)

// RelayRouting is for message from a remote node to another remote node or
// local node
type RelayRouting struct {
	*routing.Routing
	chord *Chord
}

// NewRelayRouting creates a new RelayRouting
func NewRelayRouting(localMsgChan chan<- *node.RemoteMessage, rxMsgChan <-chan *node.RemoteMessage, chord *Chord) (*RelayRouting, error) {
	r, err := routing.NewRouting(localMsgChan, rxMsgChan)
	if err != nil {
		return nil, err
	}

	rr := &RelayRouting{
		Routing: r,
		chord:   chord,
	}

	return rr, nil
}

// Start starts handling relay message from rxChan
func (rr *RelayRouting) Start() error {
	return rr.Routing.Start(rr, RelayRoutingNumWorkers)
}

// GetNodeToRoute returns the local node and remote nodes to route message to
func (rr *RelayRouting) GetNodeToRoute(remoteMsg *node.RemoteMessage) (*node.LocalNode, []*node.RemoteNode, error) {
	succ := rr.chord.successors.GetFirst()
	if succ == nil || BetweenLeftIncl(rr.chord.LocalNode.Id, succ.Id, remoteMsg.Msg.DestId) {
		return rr.chord.LocalNode, nil, nil
	}

	for i := len(rr.chord.fingerTable) - 1; i >= 0; i-- {
		finger := rr.chord.fingerTable[i]
		if finger.IsEmpty() {
			continue
		}

		var nextHop *node.RemoteNode
		maxPriority := float64(math.MinInt64)
		for _, rn := range finger.ToRemoteNodeList(true) {
			if !rn.IsOutbound && !rr.chord.predecessors.Exists(rn.Id) {
				continue
			}
			if BetweenIncl(rr.chord.LocalNode.Id, remoteMsg.Msg.DestId, rn.Id) {
				priority := float64(math.MinInt32)
				rtt := rn.GetRoundTripTime()
				if rtt > 0 {
					priority = -float64(rtt) / float64(time.Millisecond)
				}
				var ok bool
				for _, mw := range rr.chord.middlewareStore.relayPriority {
					if priority, ok = mw.Func(rn, priority); !ok {
						break
					}
				}
				if priority >= maxPriority {
					nextHop = rn
					maxPriority = priority
				}
			}
		}

		if nextHop != nil {
			return nil, []*node.RemoteNode{nextHop}, nil
		}
	}

	successors := rr.chord.successors.ToRemoteNodeList(true)
	for i := 0; i < len(successors)-1; i++ {
		if successors[i] == remoteMsg.RemoteNode {
			continue
		}
		if BetweenLeftIncl(successors[i].Id, successors[i+1].Id, remoteMsg.Msg.DestId) {
			return nil, []*node.RemoteNode{successors[i]}, nil
		}
	}

	return nil, []*node.RemoteNode{succ}, nil
}
