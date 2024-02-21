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

// TODO (3A, 3B): define your message types as structs
// Message types sent and received by the actor.

type MGet struct {
	Sender *actor.ActorRef
	Key    string
}

type MList struct {
	Sender *actor.ActorRef
	Prefix string
}

type MPut struct {
	Sender    *actor.ActorRef
	Key       string
	Value     string
	TimeStamp int64
}

type MGetResult struct {
	Value string
	Ok    bool
}

type MListResult struct {
	Entries map[string]string
}

// Reply for Put RPC.
type MPutResult struct {
	Count int
}

type MInit struct {
	Leader          *actor.ActorRef
	IsLeader        bool
	LocalActorRefs  []*actor.ActorRef
	RemoteActorRefs []*actor.ActorRef
	SystemRef       *actor.ActorRef
}

type MUpdate struct {
	NewChange       map[string]ValueWithTimestamp
	RemoteActorRefs []*actor.ActorRef
}

type MUpdateRemote struct {
	RemoteActorRefs []*actor.ActorRef
}

func init() {
	// TODO (3A, 3B): Register message types, e.g.:
	// gob.Register(MyMessage1{})
	gob.Register(MGet{})
	gob.Register(MList{})
	gob.Register(MPut{})
	gob.Register(MGetResult{})
	gob.Register(MListResult{})
	gob.Register(MPutResult{})
	gob.Register(MInit{})
	gob.Register(MUpdate{})
	gob.Register(MUpdateRemote{})
}

type ValueWithTimestamp struct {
	Value     string
	TimeStamp int64
}

type queryActor struct {
	// TODO (3A, 3B): implement this!
	context *actor.ActorContext
	// data       map[string]string
	// timeStamps map[string]int64
	data               map[string]ValueWithTimestamp
	localNewChange     map[string]ValueWithTimestamp // record new changes only in leader
	remoteNewChange    map[string]ValueWithTimestamp
	lock               sync.Mutex
	Leader             *actor.ActorRef
	IsLeader           bool
	LocalActorRefs     []*actor.ActorRef
	RemoteActorRefs    []*actor.ActorRef
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
		// TODO (3A, 3B): implement this!
	}

	return actor

}

// OnMessage implements actor.Actor.OnMessage.
func (actor *queryActor) OnMessage(message any) error {
	actor.lock.Lock()
	defer actor.lock.Unlock()
	// TODO (3A, 3B): implement this!
	switch m := message.(type) {
	case MInit:
		actor.IsInitialized = true
		actor.Leader = m.Leader
		actor.IsLeader = m.IsLeader
		actor.LocalActorRefs = m.LocalActorRefs
		if actor.IsLeader && len(actor.RemoteActorRefs) < len(m.RemoteActorRefs) {
			for _, ref := range m.RemoteActorRefs {
				if _, ok := actor.RemoteActorRefsMap[ref.Uid()]; !ok {
					actor.RemoteActorRefsMap[ref.Uid()] = ref
					actor.context.Tell(ref, MUpdate{actor.mapBeforeInit, actor.RemoteActorRefs})
				}
			}
			actor.RemoteActorRefs = m.RemoteActorRefs
		}
		if actor.IsLeader {
			go actor.leaderBroadCastRoutine()

			go actor.leaderRemoteBroadcastRoutine()
		} else {
			actor.context.Tell(actor.Leader, MUpdate{actor.mapBeforeInit, actor.RemoteActorRefs})
			go actor.actorReportRoutine()
		}
	case MUpdateRemote:
		// fmt.Println(actor.context.Self.Uid(), "received", m.RemoteActorRefs)
		if len(actor.RemoteActorRefs) < len(m.RemoteActorRefs) {
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
		//fmt.Println("received put with key:", m.Key, "and value", m.Value)
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
			actor.localNewChange[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
			actor.remoteNewChange[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
			actor.data[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
			result := MPutResult{}
			actor.context.Tell(m.Sender, result)
			return nil
		} else {
			if m.TimeStamp > actor.data[m.Key].TimeStamp {
				actor.localNewChange[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
				actor.remoteNewChange[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
				actor.data[m.Key] = ValueWithTimestamp{m.Value, m.TimeStamp}
				result := MPutResult{}
				actor.context.Tell(m.Sender, result)
				return nil
			} else {
				result := MPutResult{}
				actor.context.Tell(m.Sender, result)
				return nil
			}
		}
	case MUpdate:
		//fmt.Println(actor.context.Self.Uid(), "received222 update with content:", m.NewChange)
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

// leader broadcast the temp_data to all the other actors every 200ms
func (actor *queryActor) leaderBroadCastRoutine() {
	//fmt.Println("Starting leader broadcast routine...")
	for {
		actor.lock.Lock()
		if len(actor.localNewChange) != 0 {
			for _, ref := range actor.LocalActorRefs {
				if ref.Uid() != actor.context.Self.Uid() {
					actor.context.Tell(ref, MUpdate{actor.localNewChange, nil})
				}
			}
			actor.localNewChange = make(map[string]ValueWithTimestamp) //clear the leaderNewChange
		}

		actor.lock.Unlock()
		<-time.After(200 * time.Millisecond)
	}

}

func (actor *queryActor) actorReportRoutine() {
	//fmt.Println("Starting actor report routine...")
	for {
		actor.lock.Lock()
		actor.context.Tell(actor.Leader, MUpdate{actor.localNewChange, nil})
		actor.localNewChange = make(map[string]ValueWithTimestamp) //clear the leaderNewChange
		actor.lock.Unlock()
		<-time.After(200 * time.Millisecond)
	}
}

func (actor *queryActor) leaderRemoteBroadcastRoutine() {
	//fmt.Println("Starting leader remote broadcast routine...")
	<-time.After(500 * time.Millisecond)
	for {
		actor.lock.Lock()
		// fmt.Println(actor.context.Self.Uid(), ":length: ", actor.RemoteActorRefs, ": 921313")
		if len(actor.remoteNewChange) != 0 {
			for _, ref := range actor.RemoteActorRefs {
				if ref.Uid() != actor.context.Self.Uid() {
					actor.context.Tell(ref, MUpdate{actor.remoteNewChange, actor.RemoteActorRefs})
					// fmt.Println(actor.context.Self.Uid(), "sending to :", ref.Uid(), "with: content: ", actor.remoteNewChange)
				}
			}
			actor.remoteNewChange = make(map[string]ValueWithTimestamp) //clear the leaderNewChange
		}
		actor.lock.Unlock()
		<-time.After(250 * time.Millisecond)
	}
}
