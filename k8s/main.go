package main

import (
	"flag"
	"fmt"
	"time"
	"os"
	"net"
	
	"github.com/golang/glog"

	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/rest"
)

const ElectionKey = "election-example"

var (
	IdentityName		string
	ElectionName		string
	ElectionNamespace	string
)

func init(){
	flag.StringVar(&IdentityName, "id", "node1", "IdentityID for this instance.")
	flag.StringVar(&ElectionName, "name", "test-operator", "electionName for this instance.")
	flag.StringVar(&ElectionNamespace, "namespace", "default", "election resource's Namespace.")	
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

func InClusterConfig() (*rest.Config, error) {
	// Work around https://github.com/kubernetes/kubernetes/issues/40973
	// See https://github.com/coreos/etcd-operator/issues/731#issuecomment-283804819
	if len(os.Getenv("KUBERNETES_SERVICE_HOST")) == 0 {
		addrs, err := net.LookupHost("kubernetes.default.svc")
		if err != nil {
			panic(err)
		}
		os.Setenv("KUBERNETES_SERVICE_HOST", addrs[0])
	}
	if len(os.Getenv("KUBERNETES_SERVICE_PORT")) == 0 {
		os.Setenv("KUBERNETES_SERVICE_PORT", "443")
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func MustNewKubeClient() kubernetes.Interface {
	cfg, err := InClusterConfig()
	if err != nil {
		panic(err)
	}
	return kubernetes.NewForConfigOrDie(cfg)
}


func main(){
	kubeclient := MustNewKubeClient()
	
	rl, err := resourcelock.New(resourcelock.EndpointsResourceLock,
		ElectionNamespace,
		ElectionKey,
		kubeclient.Core(),
		resourcelock.ResourceLockConfig{
			Identity:      IdentityName,
			EventRecorder: createRecorder(kubeclient, ElectionName, ElectionNamespace),
		})
	if err != nil {
		glog.Fatalf("error creating lock: %v", err)
	}

	leaderelection.RunOrDie(leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: run,
			OnStoppedLeading: func() {
				glog.Fatalf("leader election lost")
			},
		},
	})
}

func createRecorder(kubecli kubernetes.Interface, name, namespace string) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubecli.Core().RESTClient()).Events(namespace)})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: name})
}
