package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
	
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/watch"
)

const (
	ConsulURI	= "10.0.90.30:8500"
	LeaderKey	= "leaderName"
)

var (
	IdentityName	string
)

func init(){
	flag.StringVar(&IdentityName, "id", "node1", "IdentityID for this instance.")
	flag.Parse()
}

func run(stopCh <-chan struct{}){
	for {
		select {
			case <-stopCh:
				return
			default:
				fmt.Printf("%v\n", time.Now())	
		}
		time.Sleep(1 * time.Second)
	}
}

type LeaderElection struct {
	Client	 	*api.Client
	TTL      	time.Duration
	key      	string	
	CallBack 	func(stop <-chan struct{})
	StopCh		chan struct{}
}

func (le *LeaderElection)watchKey(key string, ch chan int)error{
	params := make(map[string]interface{})
	params["type"] = "key"
	params["key"] = key
	params["stale"] = false	
	
	plan, err := watch.Parse(params)
	if err != nil {
		return err
	}
	
	plan.Handler = func(index uint64, result interface{}) {
		if kvpair, ok := result.(*api.KVPair); ok {
			if kvpair.Session == "" {
				fmt.Printf("The key %s's lock session is released.\n", key)
				ch <- 1
			}
		}
	}

	err = plan.Run(ConsulURI)
	if err != nil {
		return err
	}
	
	return nil
}

func (le *LeaderElection)Run(){
	se := &api.SessionEntry{
		Name:      IdentityName,
		TTL:       le.TTL.String(),
		LockDelay: time.Nanosecond,
	}
	for {
		sessionId, _, err :=  le.Client.Session().CreateNoChecks(se, nil)
		if err != nil {
			fmt.Printf("[%s] Create Session failed: %v", IdentityName, err)
			continue
		}
		
		kvpair := &api.KVPair{
			Key 	: LeaderKey,
			Value 	: []byte(IdentityName),
			Session : sessionId,
		}
		locked, _, err := le.Client.KV().Acquire(kvpair, nil)
		if err != nil {
			fmt.Printf("[%s] Acquire Lock key %s failed: %v", IdentityName, LeaderKey, err)
			continue			
		}
		
		if !locked {
			fmt.Printf("[%s] is follower. Begin watch the key.\n", IdentityName)
			ch := make(chan int, 1)
			go le.watchKey(LeaderKey, ch)
			<-ch
			fmt.Printf("[%s] The lock is release, Again election.\n", IdentityName)
		} else {
			fmt.Printf("[%s] is leader now.\n", IdentityName)
			go le.CallBack(le.StopCh)
			le.Client.Session().RenewPeriodic(se.TTL, sessionId, nil, nil)
		}
	}	
}

func main(){
	consulConfig := &api.Config{
		Address: ConsulURI,
	}
	client, err := api.NewClient(consulConfig)
	if err != nil {
		panic(err)
	}
	
	stopCh := make(chan struct{})
	le := &LeaderElection{
		Client  	: client,
		TTL			: 10 * time.Second,
		CallBack 	: run,
		StopCh		: stopCh,
	}
	go le.Run()
	
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	close(stopCh)
}