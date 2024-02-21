package actor

import (
	"fmt"
	"net/rpc"
	"sync"
)

// global variable
// map client to count
var mapClientCount = make(map[*rpc.Client]int)
var mapClientId = make(map[*rpc.Client]int)
var clientCount = 0
var lock = sync.Mutex{}

// Calls system.tellFromRemote(ref, mars) on the remote ActorSystem listening
// on ref.Address.
//
// This function should NOT wait for a reply from the remote system before
// returning, to allow sending multiple messages in a row more quickly.
// It should ensure that messages are delivered in-order to the remote system.
// (You may assume that remoteTell is not called multiple times
// concurrently with the same ref.Address).
func remoteTell(client *rpc.Client, ref *ActorRef, mars []byte) {
	if client == nil || ref == nil {
		fmt.Println("Invalid client or ActorRef")
		return
	}

	lock.Lock()
	clientId, exists := mapClientId[client]
	if !exists {
		clientId = clientCount
		mapClientId[client] = clientId
		clientCount++
	}
	var Reply bool // The reply can be a simple acknowledgment, like a boolean.
	go client.Call("RemoteTellHandler.RemoteTell", Args{Ref: ref, Mars: mars, Count: mapClientCount[client], Client: clientId}, &Reply)
	mapClientCount[client]++

	lock.Unlock()
}

// Registers an RPC handler on server for remoteTell calls to system.
//
// You do not need to start the server's listening on the network;
// just register a handler struct that handles remoteTell RPCs by calling
// system.tellFromRemote(ref, mars).
func registerRemoteTells(system *ActorSystem, server *rpc.Server) error {
	if system == nil || server == nil {
		return fmt.Errorf("system or server is nil")
	}

	handler := &RemoteTellHandler{
		System:           system,
		remoteMessageMap: make(map[int]map[int]Args),
		nextExpect:       make(map[int]int),
	}
	return server.RegisterName("RemoteTellHandler", handler)

}

type Args struct {
	Ref    *ActorRef
	Mars   []byte
	Count  int
	Client int
}

// RemoteTellHandler handles remoteTell RPCs.
type RemoteTellHandler struct {
	System           *ActorSystem
	remoteMessageMap map[int]map[int]Args
	nextExpect       map[int]int // next expected message count
	lock             sync.Mutex
}

// RemoteTell implements the RemoteTellHandler.RemoteTell RPC method.
func (h *RemoteTellHandler) RemoteTell(msg Args, reply *bool) error {
	h.lock.Lock()
	if _, exists := h.remoteMessageMap[msg.Client]; !exists {
		h.remoteMessageMap[msg.Client] = make(map[int]Args) // Make sure the client is initialized
	}
	h.remoteMessageMap[msg.Client][msg.Count] = msg
	h.lock.Unlock()
	for {
		findExpect := false
		h.lock.Lock()
		for k, v := range h.remoteMessageMap[msg.Client] {
			if k == h.nextExpect[msg.Client] {
				h.System.tellFromRemote(v.Ref, v.Mars)
				//delete the key
				delete(h.remoteMessageMap[msg.Client], k)
				h.nextExpect[msg.Client]++
				findExpect = true
				break
			}
		}
		h.lock.Unlock()
		if !findExpect {
			break
		}
	}
	*reply = true // Acknowledgment
	return nil
}
