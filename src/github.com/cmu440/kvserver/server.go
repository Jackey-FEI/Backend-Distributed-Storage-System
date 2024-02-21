// Package kvserver implements the backend server for a
// geographically distributed, highly available, NoSQL key-value store.
package kvserver

import (
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"

	"github.com/cmu440/actor"
)

// A single server in the key-value store, running some number of
// query actors - nominally one per CPU core. Each query actor
// provides a key/value storage service on its own port.
//
// Different query actors (both within this server and across connected
// servers) periodically sync updates (Puts) following an eventually
// consistent, last-writer-wins strategy.
type Server struct {
	// TODO (3A, 3B): implement this!
	QueryActors []*actor.ActorRef // a list of queryActors, index 0 is leader
	leader      *actor.ActorRef
	leaders     []*actor.ActorRef
}

// OPTIONAL: Error handler for ActorSystem.OnError.
//
// Print the error or call debug.PrintStack() in this function.
// When starting an ActorSystem, call ActorSystem.OnError(errorHandler).
// This can help debug server-side errors more easily.
func errorHandler(err error) {
}

// Starts a server running queryActorCount query actors.
//
// The server's actor system listens for remote messages (from other actor
// systems) on startPort. The server listens for RPCs from kvclient.Clients
// on ports [startPort + 1, startPort + 2, ..., startPort + queryActorCount].
// Each of these "query RPC servers" answers queries by asking a specific
// query actor.
//
// remoteDescs contains a "description" string for each existing server in the
// key-value store. Specifically, each slice entry is the desc returned by
// an existing server's own NewServer call. The description strings are opaque
// to callers, but typically an implementation uses JSON-encoded data containing,
// e.g., actor.ActorRef's that remote servers' actors should contact.
//
// Before returning, NewServer starts the ActorSystem, all query actors, and
// all query RPC servers. If there is an error starting anything, that error is
// returned instead.
func NewServer(startPort int, queryActorCount int, remoteDescs []string) (server *Server, desc string, err error) {
	// TODO (3A, 3B): implement this!
	// fmt.Println(remoteDescs, "remoteDescs")
	leaders := make([]*actor.ActorRef, 0)
	for _, desc := range remoteDescs {
		// get ref from desc, descs are json encoded actor ref
		ref, _ := decodeActorRef(desc)
		// fmt.Println("NewServer ref", ref)
		leaders = append(leaders, ref)
	}
	// Tips:
	// - The "HTTP service" example in the net/rpc docs does not support
	// multiple RPC servers in the same process. Instead, use the following
	// template to start RPC servers (adapted from
	// https://groups.google.com/g/Golang-Nuts/c/JTn3LV_bd5M/m/cMO_DLyHPeUJ ):
	//
	system, err := actor.NewActorSystem(startPort)
	if err != nil {
		return nil, "", fmt.Errorf("error starting ActorSystem: %v", err)
	}

	server = &Server{
		QueryActors: make([]*actor.ActorRef, queryActorCount),
		leader:      nil,
		leaders:     leaders,
	}

	// Start query actors
	for i := 0; i < queryActorCount; i++ {
		ref := system.StartActor(newQueryActor)
		server.QueryActors[i] = ref
		if i == 0 {
			server.leader = server.QueryActors[0]
		} else {
			system.Tell(ref, MInit{
				Leader:   server.leader,
				IsLeader: false,
			})
		}
	}
	server.leaders = append(server.leaders, server.leader) // add this system's leader to leader list
	system.Tell(server.leader, MInit{                      //leader will broadcast, in case broadcast to nil actor
		Leader:          server.leader,
		IsLeader:        true,
		LocalActorRefs:  server.QueryActors,
		RemoteActorRefs: server.leaders,
		SystemRef:       server.QueryActors[0],
	})

	// braodcast the remote actor to all the other actors
	// fmt.Println(server.leader.Uid(), "update leaders list..")
	for _, ref := range server.leaders {
		if ref.Uid() != server.leader.Uid() {
			// fmt.Println(server.leader.Uid(), "broadcasts to", ref.Uid(), "with", server.leaders)
			system.Tell(ref, MUpdateRemote{
				RemoteActorRefs: server.leaders,
			})
		}
	}

	// Start RPC servers for each query actor
	for i := 0; i < queryActorCount; i++ {
		rpcServer := rpc.NewServer()
		port := startPort + i + 1
		queryReceiver := &queryReceiver{
			actor:       server.QueryActors[i],
			actorSystem: system,
			port:        port,
		}

		err := rpcServer.RegisterName("QueryReceiver", queryReceiver)
		if err != nil {
			return nil, "", fmt.Errorf("error registering RPC server: %v", err)
		}

		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return nil, "", fmt.Errorf("error listening on port %d: %v", port, err)
		}

		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go rpcServer.ServeConn(conn)
			}
		}()
	}
	//return leader's ref as desc
	descBytes, _ := json.Marshal(server.leader)
	desc = string(descBytes)
	return server, desc, nil
}

func decodeActorRef(encoded string) (*actor.ActorRef, error) {
	var ref actor.ActorRef
	err := json.Unmarshal([]byte(encoded), &ref)
	if err != nil {
		return nil, err
	}
	return &ref, nil
}

// OPTIONAL: Closes the server, including its actor system
// and all RPC servers.
//
// You are not required to implement this function for full credit; the tests end
// by calling Close but do not check that it does anything. However, you
// may find it useful to implement this so that you can run multiple/repeated
// tests in the same "go test" command without cross-test interference (in
// particular, old test servers' squatting on ports.)
//
// Likewise, you may find it useful to close a partially-started server's
// resources if there is an error in NewServer.
func (server *Server) Close() {
}
