// Sync Strategy:
// For local sync, we have a leader in the system, and all the other actors will report their changes to the leader every TIME_INTERVAL.
// The leader will also broadcast the changes to all the other actors every TIME_INTERVAL. We use timeStamp to determine which change is the latest.
// For remote sync, every leader will also maintain a list of all the other leaders in the system. Every leader will broadcast its changes to all the other leaders every TIME_INTERVAL.
// For the broadcast mechanism we mentioned above, We implemented this by sending itself a MLeader or MNoLeader(depends on if an actor is leader or not) message by TellAfter,
// and every time when an actor receive it, it do broadcast job and repeat sending message to itself by TellAfter.
// During the TIME_INTERVAL, every actor will record the changes from client into a temp data map,
// after the TIME_INTERVAL, the actor will send the temp data to corresponding destination (depending on whether it is a leader or normal actor) and clear the temp_data.
package kvserver

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/cmu440/actor"
)

// Implement your queryActor in this file.
// See example/counter_actor.go for an example actor using the
// github.com/cmu440/actor package.

const TIME_INTERVAL = 200 * time.Millisecond

// Message types sent and received by the actor.

// Message for Get RPC.
type MGet struct {
	Sender *actor.ActorRef
	Key    string
}

// Message for List RPC.
type MList struct {
	Sender *actor.ActorRef
	Prefix string
}

// Message for Put RPC.
type MPut struct {
	Sender    *actor.ActorRef
	Key       string
	Value     string
	TimeStamp int64
}

// Reply for Get RPC.
type MGetResult struct {
	Value string
	Ok    bool
}

// Reply for List RPC.
type MListResult struct {
	Entries map[string]string
}

// Reply for Put RPC.
type MPutResult struct {
	Count int
}

// Used to initialize the actor
type MInit struct {
	Leader          *actor.ActorRef
	IsLeader        bool
	LocalActorRefs  []*actor.ActorRef
	RemoteActorRefs []*actor.ActorRef
	SystemRef       *actor.ActorRef
}

// Used to update the actor
type MUpdate struct {
	NewChange       map[string]ValueWithTimestamp
	RemoteActorRefs []*actor.ActorRef
}

// Used to update remote leaders of actor
type MUpdateRemote struct {
	RemoteActorRefs []*actor.ActorRef
}

// used for leader tellAfter to broadcast
type MLeader struct {
	// empty struct
}

// used for normal actor tellAfter to broadcast
type MNoLeader struct {
	// empty struct
}

func init() {
	//Register message types
	gob.Register(MGet{})
	gob.Register(MList{})
	gob.Register(MPut{})
	gob.Register(MGetResult{})
	gob.Register(MListResult{})
	gob.Register(MPutResult{})
	gob.Register(MInit{})
	gob.Register(MUpdate{})
	gob.Register(MUpdateRemote{})
	gob.Register(MLeader{})
	gob.Register(MNoLeader{})
}

type ValueWithTimestamp struct {
	Value     string
	TimeStamp int64
}

type queryActor struct {
	context            *actor.ActorContext
	data               map[string]ValueWithTimestamp
	localNewChange     map[string]ValueWithTimestamp // record new changes only in leader
	remoteNewChange    map[string]ValueWithTimestamp
	lock               sync.Mutex
	Leader             *actor.ActorRef
	IsLeader           bool
	LocalActorRefs     []*actor.ActorRef // local actor refs
	RemoteActorRefs    []*actor.ActorRef // remote actor refs(leaders), only used in leader
	RemoteActorRefsMap map[string]*actor.ActorRef
	IsInitialized      bool
	mapBeforeInit      map[string]ValueWithTimestamp // before initial message comes in
}

// "Constructor" for queryActors, used in ActorSystem.StartActor.
func newQueryActor(context *actor.ActorContext) actor.Actor {
	actor := &queryActor{
		context:            context,
		data:               make(map[string]ValueWithTimestamp),
		localNewChange:     make(map[string]ValueWithTimestamp),
		remoteNewChange:    make(map[string]ValueWithTimestamp),
		mapBeforeInit:      make(map[string]ValueWithTimestamp),
		lock:               sync.Mutex{},
		LocalActorRefs:     make([]*actor.ActorRef, 0),
		RemoteActorRefs:    make([]*actor.ActorRef, 0),
		RemoteActorRefsMap: make(map[string]*actor.ActorRef),
	}
	return actor

}

// OnMessage implements actor.Actor.OnMessage.
func (actor *queryActor) OnMessage(message any) error {
	actor.lock.Lock()
	defer actor.lock.Unlock()
	switch m := message.(type) {
	case MInit:
		actor.IsInitialized = true
		actor.Leader = m.Leader
		actor.IsLeader = m.IsLeader
		actor.LocalActorRefs = m.LocalActorRefs
		if actor.IsLeader && len(actor.RemoteActorRefs) < len(m.RemoteActorRefs) { // leader update the remote actor refs
			for _, ref := range m.RemoteActorRefs {
				if _, ok := actor.RemoteActorRefsMap[ref.Uid()]; !ok {
					actor.RemoteActorRefsMap[ref.Uid()] = ref
					actor.context.Tell(ref, MUpdate{actor.mapBeforeInit, actor.RemoteActorRefs})
				}
			}
			actor.RemoteActorRefs = m.RemoteActorRefs
		}
		if !actor.IsLeader {
			actor.context.Tell(actor.Leader, MUpdate{actor.mapBeforeInit, actor.RemoteActorRefs})
		}
	case MLeader:
		actor.leaderBroadCastFunc()
		actor.leaderRemoteBroadcastFunc()
		//send to itself MLeader after TIME_INTERVAL use TellAfter
		actor.context.TellAfter(actor.context.Self, MLeader{}, TIME_INTERVAL)
	case MNoLeader:
		actor.actorReportFunc()
		// send to itself MNoLeader after TIME_INTERVAL use TellAfter
		actor.context.TellAfter(actor.context.Self, MNoLeader{}, TIME_INTERVAL)
	case MUpdateRemote:
		if len(actor.RemoteActorRefs) < len(m.RemoteActorRefs) { //update the remote actor refs
			for _, ref := range m.RemoteActorRefs {
				if _, ok := actor.RemoteActorRefsMap[ref.Uid()]; !ok {
					actor.RemoteActorRefsMap[ref.Uid()] = ref
					actor.context.Tell(ref, MUpdate{actor.data, actor.RemoteActorRefs})
				}
			}
			actor.RemoteActorRefs = m.RemoteActorRefs
		}
	case MGet:
		_, ok := actor.data[m.Key]
		if !ok {
			result := MGetResult{"", false}
			actor.context.Tell(m.Sender, result)
		} else {
			result := MGetResult{actor.data[m.Key].Value, true}
			actor.context.Tell(m.Sender, result)
		}
	case MList:
		entries := make(map[string]string)
		for k, v := range actor.data {
			if len(k) >= len(m.Prefix) && k[:len(m.Prefix)] == m.Prefix {
				entries[k] = v.Value
			}
		}
		result := MListResult{entries}
		actor.context.Tell(m.Sender, result)
	case MPut:
		if !actor.IsInitialized {
			if _, ok := actor.data[m.Key]; !ok {
				actor.mapBeforeInit[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
				actor.data[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
			} else {
				if actor.data[m.Key].TimeStamp < m.TimeStamp {
					actor.mapBeforeInit[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
					actor.data[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
				}
			}
			result := MPutResult{}
			actor.context.Tell(m.Sender, result)
			return nil
		}
		//check if current key is in the map
		if _, ok := actor.data[m.Key]; !ok {
			// if not, add it to the map
			actor.localNewChange[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
			actor.remoteNewChange[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
			actor.data[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
			result := MPutResult{}
			actor.context.Tell(m.Sender, result)
			return nil
		} else {
			if m.TimeStamp > actor.data[m.Key].TimeStamp {
				// if the timestamp is larger, update the value
				actor.localNewChange[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
				actor.remoteNewChange[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
				actor.data[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
				result := MPutResult{}
				actor.context.Tell(m.Sender, result)
				return nil
			} else {
				// if the timestamp is smaller, do nothing
				result := MPutResult{}
				actor.context.Tell(m.Sender, result)
				return nil
			}
		}
	case MUpdate:
		if actor.IsLeader {
			if len(actor.RemoteActorRefs) < len(m.RemoteActorRefs) {
				actor.RemoteActorRefs = m.RemoteActorRefs
			}
		}
		for key, value := range m.NewChange {
			if _, ok := actor.data[key]; !ok {
				actor.data[key] = value
				if actor.IsLeader {
					actor.localNewChange[key] = value
					actor.remoteNewChange[key] = value
				}
			} else {
				if actor.data[key].TimeStamp < value.TimeStamp {
					actor.data[key] = value
					if actor.IsLeader {
						actor.localNewChange[key] = value
						actor.remoteNewChange[key] = value
					}
				}
			}
		}
	default:
		// Return an error. The ActorSystem will report this but not
		// do anything else.
		return fmt.Errorf("Unexpected counterActor message type: %T", m)
	}
	return nil

}

// leader broadcast the temp_data to the other actors
func (actor *queryActor) leaderBroadCastFunc() {
	if len(actor.localNewChange) != 0 {
		for _, ref := range actor.LocalActorRefs {
			if ref.Uid() != actor.context.Self.Uid() {
				actor.context.Tell(ref, MUpdate{actor.localNewChange, nil})
			}
		}
		actor.localNewChange = make(map[string]ValueWithTimestamp) //clear localNewChange
	}
}

// actor report the temp_data to leader
func (actor *queryActor) actorReportFunc() {
	actor.context.Tell(actor.Leader, MUpdate{actor.localNewChange, nil})
	actor.localNewChange = make(map[string]ValueWithTimestamp) //clear the localNewChange
}

// leader broadcast the temp_data to all the other leaders
func (actor *queryActor) leaderRemoteBroadcastFunc() {
	if len(actor.remoteNewChange) != 0 {
		for _, ref := range actor.RemoteActorRefs {
			if ref.Uid() != actor.context.Self.Uid() {
				actor.context.Tell(ref, MUpdate{actor.remoteNewChange, actor.RemoteActorRefs})
			}
		}
		actor.remoteNewChange = make(map[string]ValueWithTimestamp) //clear the remoteNewChange
	}
}
