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
	chanRef, respCh := rcvr.actorSystem.NewChannelRef()
	rcvr.actorSystem.Tell(rcvr.actor, MGet{Sender: chanRef, Key: args.Key})
	ans := (<-respCh).(MGetResult)
	reply.Value = ans.Value
	reply.Ok = ans.Ok
	return nil
}

// List implements kvcommon.QueryReceiver.List.
func (rcvr *queryReceiver) List(args kvcommon.ListArgs, reply *kvcommon.ListReply) error {
	chanRef, respCh := rcvr.actorSystem.NewChannelRef()
	rcvr.actorSystem.Tell(rcvr.actor, MList{Sender: chanRef, Prefix: args.Prefix})
	ans := (<-respCh).(MListResult)
	reply.Entries = ans.Entries
	return nil
}

// Put implements kvcommon.QueryReceiver.Put.
func (rcvr *queryReceiver) Put(args kvcommon.PutArgs, reply *kvcommon.PutReply) error {
	chanRef, respCh := rcvr.actorSystem.NewChannelRef()
	rcvr.actorSystem.Tell(rcvr.actor, MPut{Sender: chanRef, Key: args.Key, Value: args.Value, TimeStamp: time.Now().UnixNano()})
	ans := (<-respCh).(MPutResult)
	ans.Count = ans.Count + 1
	return nil
}
