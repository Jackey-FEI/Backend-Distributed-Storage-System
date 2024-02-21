package kvserver

import (
	"time"

	"github.com/cmu440/actor"
	"github.com/cmu440/kvcommon"
)

// RPC handler implementing the kvcommon.QueryReceiver interface.
// There is one queryReceiver per queryActor, each running on its own port,
// created and registered for RPCs in NewServer.
//
// A queryReceiver MUST answer RPCs by sending a message to its query
// actor and getting a response message from that query actor (via
// ActorSystem's NewChannelRef). It must NOT attempt to answer queries
// using its own state, and it must NOT directly coordinate with other
// queryReceivers - all coordination is done within the actor system
// by its query actor.
type queryReceiver struct {
	// TODO (3A): implement this!
	actor       *actor.ActorRef
	actorSystem *actor.ActorSystem
	port        int
}

// Get implements kvcommon.QueryReceiver.Get.
func (rcvr *queryReceiver) Get(args kvcommon.GetArgs, reply *kvcommon.GetReply) error {
	// TODO (3A): implement this!
	//fmt.Println("Getting the actor for its value...")
	chanRef, respCh := rcvr.actorSystem.NewChannelRef()
	rcvr.actorSystem.Tell(rcvr.actor, MGet{Sender: chanRef, Key: args.Key})
	ans := (<-respCh).(MGetResult)
	// fmt.Println("Actor responded", ans.Value, "(should be 3) revise this print")
	reply.Value = ans.Value
	reply.Ok = ans.Ok
	// if !ans.Ok {
	// 	return fmt.Errorf("key doesn't exist")
	// }
	return nil
}

// List implements kvcommon.QueryReceiver.List.
func (rcvr *queryReceiver) List(args kvcommon.ListArgs, reply *kvcommon.ListReply) error {
	// TODO (3A): implement this!
	// fmt.Println("Listing the actor for its value...")
	chanRef, respCh := rcvr.actorSystem.NewChannelRef()
	rcvr.actorSystem.Tell(rcvr.actor, MList{Sender: chanRef, Prefix: args.Prefix})
	ans := (<-respCh).(MListResult)
	// fmt.Println("Actor responded", ans.Entries, "(should be 3) revise this print")
	reply.Entries = ans.Entries
	return nil
}

// Put implements kvcommon.QueryReceiver.Put.
func (rcvr *queryReceiver) Put(args kvcommon.PutArgs, reply *kvcommon.PutReply) error {
	// TODO (3A): implement this!
	// fmt.Println("Putting the actor for its value:", args.Key, ": ", args.Value)
	chanRef, respCh := rcvr.actorSystem.NewChannelRef()
	rcvr.actorSystem.Tell(rcvr.actor, MPut{Sender: chanRef, Key: args.Key, Value: args.Value, TimeStamp: time.Now().UnixNano()})
	ans := (<-respCh).(MPutResult)
	ans.Count = ans.Count + 1
	//fmt.Println("Actor responded put: ", ans)
	return nil
}
