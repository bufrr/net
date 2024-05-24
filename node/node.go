package node

import (
	"fmt"
	"sync"

	"github.com/bufrr/net/common"
	protobuf "github.com/bufrr/net/protobuf"
)

// Node is a remote or local node
type Node struct {
	sync.RWMutex
	*protobuf.Node
	common.LifeCycle
}

func newNode(n *protobuf.Node) (*Node, error) {
	node := &Node{
		Node: n,
	}
	return node, nil
}

// NewNode creates a node
func NewNode(id []byte, addr string) (*Node, error) {
	n := &protobuf.Node{
		Id:   id,
		Addr: addr,
	}
	return newNode(n)
}

func (n *Node) String() string {
	return fmt.Sprintf("%x@%s", n.Id, n.Addr)
}
