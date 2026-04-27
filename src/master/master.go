package main

import (
	"bufio"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

var portnum *int = flag.Int("port", 7087, "Port # to listen on. Defaults to 7087")
var numNodes *int = flag.Int("N", 3, "Number of replicas. Defaults to 3.")
var twoLeaders *bool = flag.Bool("twoLeaders", false, "Two leaders for slowdown tolerance. Defaults to false.")
var fvc *bool = flag.Bool("fvc", false, "fast-view-change. Defaults to false.")
var serversFile *string = flag.String("f", "", "fast-view-change. Defaults to false.")

const PING_INTERVAL = 10 * time.Millisecond

const VIEW_CHANGE_TIMEOUT = 300 * time.Millisecond

type Master struct {
	N        int
	nodeList []string
	addrList []string
	portList []int
	lock     *sync.Mutex
	nodes    []*rpc.Client
	leader   []bool
	alive    []bool
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	log.Printf("Master starting on port %d\n", *portnum)
	log.Printf("...waiting for %d replicas\n", *numNodes)
	log.Printf("twoleaders %v fvc %v PING_INTERVAL %v VIEW_CHANGE_TIMEOUT %v\n",
		*twoLeaders, *fvc, PING_INTERVAL, VIEW_CHANGE_TIMEOUT)

	master := &Master{*numNodes,
		make([]string, 0, *numNodes),
		make([]string, 0, *numNodes),
		make([]int, 0, *numNodes),
		new(sync.Mutex),
		make([]*rpc.Client, *numNodes),
		make([]bool, *numNodes),
		make([]bool, *numNodes)}

	rpc.Register(master)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("Master listen error:", err)
	}

	go master.run()

	http.Serve(l, nil)
}

func (master *Master) run() {
	for true {
		master.lock.Lock()
		if len(master.nodeList) == master.N {
			master.lock.Unlock()
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)
	}
	time.Sleep(2000000000)

	log.Printf("ABOUT TO CONNECT TO SERVERS: %v\n", *master)
	// connect to SMR servers
	for i := 0; i < master.N; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i]+1000)
		master.nodes[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to replica %d addr %v\n", i, addr)
		}
		master.leader[i] = false
	}
	master.leader[0] = true

	if master.N >= 2 && *twoLeaders {
		master.leader[1] = true
	}

	currentLeader := 0 // only for fvc
	for true {
		if *fvc {
			time.Sleep(PING_INTERVAL)
			new_leader := false
			// this pings them one-by-one... bruh
			for i, node := range master.nodes {
				// let's only ping the leader for now
				if !master.leader[i] {
					master.alive[i] = true
					continue
				}
				done := make(chan bool, 1)
				to := time.NewTimer(VIEW_CHANGE_TIMEOUT)
				go func() {
					err := node.Call("Replica.Ping", new(genericsmrproto.PingArgs), new(genericsmrproto.PingReply))
					if err != nil {
						done <- true
					} else {
						done <- false
					}
				}()

				// todo: add view number
				select {
				case hasError := <-done:
					if hasError {
						master.alive[i] = false
						if master.leader[i] {
							new_leader = true
							master.leader[i] = false
						}
					} else {
						master.alive[i] = true
					}
				case <-to.C:
					if master.leader[i] {
						new_leader = true
						master.leader[i] = false
					}
				}
				/*err := node.Call("Replica.Ping", new(genericsmrproto.PingArgs), new(genericsmrproto.PingReply))
				if err != nil {
					//log.Printf("Replica %d has failed to reply\n", i)
					master.alive[i] = false
					if master.leader[i] {
						// neet to choose a new leader
						new_leader = true
						master.leader[i] = false
					}
				} else {
					master.alive[i] = true
				}*/
			}
			if !new_leader {
				continue
			}
			log.Printf("Looking for new leader...\n")
			for i := 1; i < len(master.nodes); i++ {
				newLeader := (currentLeader + i) % 2 // cch added, heterogeneity 1 slowdown was: len(master.nodes)
				log.Printf("Checking node %v alive? %v\n", newLeader, master.alive[newLeader])
				if master.alive[newLeader] {
					err := master.nodes[newLeader].Call("Replica.BeTheLeader",
						new(genericsmrproto.BeTheLeaderArgs), new(genericsmrproto.BeTheLeaderReply))
					if err == nil {
						master.leader[newLeader] = true
						log.Printf("Replica %d is the new leader.", newLeader)
						currentLeader = newLeader
						break
					}
				}
			}
			// *** END FVC *** //
		} else {
			time.Sleep(3000 * 1000 * 1000) // 3 seconds
			new_leader := false
			for i, node := range master.nodes {
				err := node.Call("Replica.Ping", new(genericsmrproto.PingArgs), new(genericsmrproto.PingReply))
				if err != nil {
					//log.Printf("Replica %d has failed to reply\n", i)
					master.alive[i] = false
					if master.leader[i] {
						// neet to choose a new leader
						new_leader = true
						master.leader[i] = false
					}
				} else {
					master.alive[i] = true
				}
			}
			if !new_leader {
				continue
			}
			for i, new_master := range master.nodes {
				if master.alive[i] {
					err := new_master.Call("Replica.BeTheLeader",
						new(genericsmrproto.BeTheLeaderArgs), new(genericsmrproto.BeTheLeaderReply))
					if err == nil {
						master.leader[i] = true
						// log.Printf("Replica %d is the new leader.", i)
						break
					}
				}
			}
		}
	}
}

func (master *Master) Register(args *masterproto.RegisterArgs, reply *masterproto.RegisterReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	nlen := len(master.nodeList)
	index := nlen

	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)

	for i, ap := range master.nodeList {
		if addrPort == ap {
			index = i
			break
		}
	}

	// if new replica
	if index == nlen {
		master.nodeList = master.nodeList[0 : nlen+1]
		master.nodeList[nlen] = addrPort
		master.addrList = master.addrList[0 : nlen+1]
		master.addrList[nlen] = args.Addr
		master.portList = master.portList[0 : nlen+1]
		master.portList[nlen] = args.Port
		nlen++
	}

	if *serversFile != "" {
		// reset to ips in file and refind index for the last one
		if nlen == master.N {
			file, err := os.Open(*serversFile)
			if err != nil {
				log.Panicf("error opening servers %v\n", err)
			}
			defer file.Close()

			var nodesAddr []string
			var nodesAddrPort []string

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" || strings.HasPrefix(line, "#") {
					continue // skip blanks and comments
				}

				host, port, err := net.SplitHostPort(line)
				if err != nil {
					log.Panicf("invalid server entry %q in %s: %v (expected host:port; IPv6 must be like [::1]:7010)", line, *serversFile, err)
				}

				nodesAddr = append(nodesAddr, host)
				nodesAddrPort = append(nodesAddrPort, net.JoinHostPort(host, port))
			}
			if err := scanner.Err(); err != nil {
				log.Panicf("error reading %s: %v", *serversFile, err)
			}

			if len(nodesAddrPort) != master.N {
				log.Panicf("server count mismatch: file has %d entries, but master.N=%d", len(nodesAddrPort), master.N)
			}

			log.Printf("Servers from file are %v %v\n", nodesAddr, nodesAddrPort)

			for i := range nodesAddr {
				master.nodeList[i] = nodesAddrPort[i] // host:port
				master.addrList[i] = nodesAddr[i]     // host only
			}

			for i, ap := range master.nodeList {
				if addrPort == ap {
					index = i
					break
				}
			}
		}
	}

	// For deterministic replica ID assignment for WAN experiments...
	// if *serversFile != "" {
	// 	// reset to ips in file and refind index for the last one
	// 	if nlen == master.N {
	// 		file, err := os.Open(*serversFile)
	// 		if err != nil {
	// 			log.Panicf("error opening servers %v\n", err)
	// 		}
	// 		defer file.Close()

	// 		var nodesAddr []string
	// 		var nodesAddrPort []string
	// 		scanner := bufio.NewScanner(file)
	// 		for scanner.Scan() {
	// 			nodesAddr = append(nodesAddr, scanner.Text())
	// 			nodesAddrPort = append(nodesAddrPort, scanner.Text()+":7070")
	// 		}
	// 		log.Printf("Servers from file are %v %v\n", nodesAddr, nodesAddrPort)
	// 		// return lines, scanner.Err()

	// 		for i := range nodesAddr {
	// 			master.nodeList[i] = nodesAddrPort[i]
	// 			master.addrList[i] = nodesAddr[i]
	// 		}
	// 		// master.nodeList = nodesAddrPort
	// 		// master.addrList = nodesAddr
	// 		for i, ap := range master.nodeList {
	// 			if addrPort == ap {
	// 				index = i
	// 				break
	// 			}
	// 		}
	// 	}
	// }

	if nlen == master.N {
		reply.Ready = true
		reply.ReplicaId = index
		reply.NodeList = master.nodeList
	} else {
		reply.Ready = false
	}

	log.Printf("Registering %v, reply %v\n", *args, *reply)
	log.Printf("Master is: %v\n", *master)
	return nil
}

func (master *Master) GetLeader(args *masterproto.GetLeaderArgs, reply *masterproto.GetLeaderReply) error {
	time.Sleep(4 * 1000 * 1000) // why sleep for 4ms?
	for i, l := range master.leader {
		if l {
			*reply = masterproto.GetLeaderReply{i}
			break
		}
	}
	return nil
}

// slowdown: get two leaders
func (master *Master) GetTwoLeaders(args *masterproto.GetTwoLeadersArgs, reply *masterproto.GetTwoLeadersReply) error {

	time.Sleep(4 * 1000 * 1000)
	reply.Leader1Id = -1
	reply.Leader2Id = -1
	for i, l := range master.leader {
		if l {
			if reply.Leader1Id == -1 {
				reply.Leader1Id = i
			} else {
				reply.Leader2Id = i
				break
			}
		}
	}
	return nil
}

func (master *Master) GetReplicaList(args *masterproto.GetReplicaListArgs, reply *masterproto.GetReplicaListReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	if len(master.nodeList) == master.N {
		reply.ReplicaList = master.nodeList
		reply.Ready = true
	} else {
		reply.Ready = false
	}
	return nil
}
