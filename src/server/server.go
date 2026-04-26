package main

import (
	"avicenna"
	"copilot"
	"epaxos"
	"flag"
	"fmt"
	"fvc"
	"gpaxos"
	"latentcopilot"
	"log"
	"masterproto"
	"mencius"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"paxos"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

var portnum *int = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")
var doMencius *bool = flag.Bool("m", false, "Use Mencius as the replication protocol. Defaults to false.")
var doGpaxos *bool = flag.Bool("g", false, "Use Generalized Paxos as the replication protocol. Defaults to false.")
var doEpaxos *bool = flag.Bool("e", false, "Use EPaxos as the replication protocol. Defaults to false.")
var doCopilot *bool = flag.Bool("copilot", false, "Use Copilot as the replication protocol. Defaults to false.")
var doLatentCopilot *bool = flag.Bool("latentcopilot", false, "Use Latent Copilot as the replication protocol. Defaults to false.")

// var doMR99rsm = flag.Bool("domr99rsm", false, "Use MR99RSM as the replication protocol. Defaults to false.")
var doAvicenna = flag.Bool("doavicenna", false, "Use Avicenna as the replication protocol. Defaults to false.")
var doFvc = flag.Bool("dofvc", false, "Use FVC as teh replica protocol. Defaults to false.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var cpuprofile *string = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty *bool = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var exec *bool = flag.Bool("exec", false, "Execute commands.")
var dreply *bool = flag.Bool("dreply", false, "Reply to client only after command has been executed.")
var beacon *bool = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var durable *bool = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")
var rreply *bool = flag.Bool("rreply", true, "Non-leader replicas reply to client.")
var slowdownDuration *int64 = flag.Int64("slowdownduration", int64(0), "The length of the slowdown to inject\n")

var doRotate *bool = flag.Bool("dorotate", false, "If MR99RSM should rotate")

func main() {
	flag.Parse()
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	runtime.GOMAXPROCS(*procs)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %d\n", *portnum)

	replicaId, nodeList := registerWithMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort))

	if *doEpaxos {
		log.Println("Starting Egalitarian Paxos replica...")
		rep := epaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *beacon, *durable, time.Duration(*slowdownDuration)*time.Millisecond)
		rpc.Register(rep)
	} else if *doMencius {
		log.Println("Starting Mencius replica...")
		rep := mencius.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	} else if *doGpaxos {
		log.Println("Starting Generalized Paxos replica...")
		rep := gpaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply)
		rpc.Register(rep)
	} else if *doCopilot {
		log.Println("Starting Copilot replica...")
		rep := copilot.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *beacon, *durable, *rreply, time.Duration(*slowdownDuration)*time.Millisecond)
		rpc.Register(rep)
	} else if *doLatentCopilot {
		log.Println("Starting Latent Copilot replica...")
		rep := latentcopilot.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *beacon, *durable, *rreply, time.Duration(*slowdownDuration)*time.Millisecond)
		rpc.Register(rep)
	} else if *doAvicenna {
		log.Println("Starting Avicenna replica...")
		rep := avicenna.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable, time.Duration(*slowdownDuration)*time.Millisecond)
		rpc.Register(rep)
	} else if *doFvc {
		log.Println("Starting FVC replica...")
		rep := fvc.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable, time.Duration(*slowdownDuration)*time.Millisecond)
		rpc.Register(rep)

	} else {
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable, time.Duration(*slowdownDuration)*time.Millisecond)
		rpc.Register(rep)
	}

	rpc.HandleHTTP()
	//listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)
}

func registerWithMaster(masterAddr string) (int, []string) {
	args := &masterproto.RegisterArgs{*myAddr, *portnum}
	var reply masterproto.RegisterReply

	for done := false; !done; {
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.Register", args, &reply)
			if err == nil && reply.Ready == true {
				done = true
				break
			}
		}
		time.Sleep(1e9)
	}

	log.Printf("Registered with Master. My Id %v NodeList: %v\n",
		reply.ReplicaId, reply.NodeList)
	return reply.ReplicaId, reply.NodeList
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}
