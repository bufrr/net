package chord

import (
	"github.com/bufrr/net/message"
	"github.com/bufrr/net/node"
	protobuf "github.com/bufrr/net/protobuf"
	"google.golang.org/protobuf/proto"
)

// NewGetSuccAndPredMessage creates a GET_SUCC_AND_PRED message to get the
// successors and predecessor of a remote node
func NewGetSuccAndPredMessage(numSucc, numPred uint32, msgIDBytes uint8) (*protobuf.Message, error) {
	id, err := message.GenID(msgIDBytes)
	if err != nil {
		return nil, err
	}

	msgBody := &protobuf.GetSuccAndPred{
		NumSucc: numSucc,
		NumPred: numPred,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &protobuf.Message{
		MessageType: protobuf.MessageType_GET_SUCC_AND_PRED,
		RoutingType: protobuf.RoutingType_DIRECT,
		MessageId:   id,
		Message:     buf,
	}

	return msg, nil
}

// NewGetSuccAndPredReply creates a GET_SUCC_AND_PRED reply to send successors
// and predecessor
func (c *Chord) NewGetSuccAndPredReply(replyToID []byte, successors, predecessors []*protobuf.Node) (*protobuf.Message, error) {
	id, err := message.GenID(c.LocalNode.MessageIDBytes)
	if err != nil {
		return nil, err
	}

	msgBody := &protobuf.GetSuccAndPredReply{
		Successors:   successors,
		Predecessors: predecessors,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &protobuf.Message{
		MessageType: protobuf.MessageType_GET_SUCC_AND_PRED,
		RoutingType: protobuf.RoutingType_DIRECT,
		ReplyToId:   replyToID,
		MessageId:   id,
		Message:     buf,
	}

	return msg, nil
}

// NewFindSuccAndPredMessage creates a FIND_SUCC_AND_PRED message to find
// numSucc successors and numPred predecessors of a key
func (c *Chord) NewFindSuccAndPredMessage(key []byte, numSucc, numPred uint32) (*protobuf.Message, error) {
	id, err := message.GenID(c.LocalNode.MessageIDBytes)
	if err != nil {
		return nil, err
	}

	msgBody := &protobuf.FindSuccAndPred{
		Key:     key,
		NumSucc: numSucc,
		NumPred: numPred,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &protobuf.Message{
		MessageType: protobuf.MessageType_FIND_SUCC_AND_PRED,
		RoutingType: protobuf.RoutingType_DIRECT,
		MessageId:   id,
		Message:     buf,
		DestId:      key,
	}

	return msg, nil
}

// NewFindSuccAndPredReply creates a FIND_SUCC_AND_PRED reply to send successors
// and predecessors
func (c *Chord) NewFindSuccAndPredReply(replyToID []byte, successors, predecessors []*protobuf.Node) (*protobuf.Message, error) {
	id, err := message.GenID(c.LocalNode.MessageIDBytes)
	if err != nil {
		return nil, err
	}

	msgBody := &protobuf.FindSuccAndPredReply{
		Successors:   successors,
		Predecessors: predecessors,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &protobuf.Message{
		MessageType: protobuf.MessageType_FIND_SUCC_AND_PRED,
		RoutingType: protobuf.RoutingType_DIRECT,
		ReplyToId:   replyToID,
		MessageId:   id,
		Message:     buf,
	}

	return msg, nil
}

// handleRemoteMessage handles a remote message and returns if it should be
// passed through to local node and error
func (c *Chord) handleRemoteMessage(remoteMsg *node.RemoteMessage) (bool, error) {
	if remoteMsg.RemoteNode == nil {
		return true, nil
	}

	switch remoteMsg.Msg.MessageType {
	case protobuf.MessageType_GET_SUCC_AND_PRED:
		msgBody := &protobuf.GetSuccAndPred{}
		err := proto.Unmarshal(remoteMsg.Msg.Message, msgBody)
		if err != nil {
			return false, err
		}

		succs := c.successors.ToProtoNodeList(true)
		if succs != nil && uint32(len(succs)) > msgBody.NumSucc {
			succs = succs[:msgBody.NumSucc]
		}

		preds := c.predecessors.ToProtoNodeList(true)
		if preds != nil && uint32(len(preds)) > msgBody.NumPred {
			preds = preds[:msgBody.NumPred]
		}

		replyMsg, err := c.NewGetSuccAndPredReply(remoteMsg.Msg.MessageId, succs, preds)
		if err != nil {
			return false, err
		}

		err = remoteMsg.RemoteNode.SendMessageAsync(replyMsg)
		if err != nil {
			return false, err
		}

	case protobuf.MessageType_FIND_SUCC_AND_PRED:
		msgBody := &protobuf.FindSuccAndPred{}
		err := proto.Unmarshal(remoteMsg.Msg.Message, msgBody)
		if err != nil {
			return false, err
		}

		// TODO: prevent unbounded number of goroutines
		go func() {
			succs, preds, err := c.FindSuccAndPred(msgBody.Key, msgBody.NumSucc, msgBody.NumPred)
			if err != nil {
				return
			}

			replyMsg, err := c.NewFindSuccAndPredReply(remoteMsg.Msg.MessageId, succs, preds)
			if err != nil {
				return
			}

			err = remoteMsg.RemoteNode.SendMessageAsync(replyMsg)
			if err != nil {
				return
			}
		}()

	default:
		return true, nil
	}

	return false, nil
}
