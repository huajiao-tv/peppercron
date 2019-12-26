package dispatch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	v3 "go.etcd.io/etcd/clientv3"
)

func TestAgentMonitor(t *testing.T) {
	// create instance
	amCtx, stop := context.WithCancel(context.Background())
	defer stop()
	am := NewAgentMonitor(amCtx, nil)

	// create an agent node
	lease, err := config.Storage.Grant(amCtx, 2)
	if err != nil {
		t.Fatal("grant lease failed", err)
	}
	key := fmt.Sprintf(logic.AgentNodeAlive, TestAgent)
	_, err = config.Storage.Put(amCtx, key, TestTag, v3.WithLease(lease.ID))
	if err != nil {
		t.Fatal("put failed", err)
	}
	downCtx, down := context.WithCancel(amCtx)
	_, err = config.Storage.KeepAlive(downCtx, lease.ID)
	if err != nil {
		t.Fatal("keep alive failed", err)
	}

	// check nodes after 1 sec
	time.Sleep(time.Second)
	nodes := am.Get(TestTag)
	if len(nodes) == 0 {
		t.Fatal("get nodes failed")
	}
	if _, ok := nodes[TestAgent]; !ok {
		t.Fatal("get nodes failed")
	}

	// check nodes after 10 sec
	time.Sleep(10 * time.Second)
	nodes = am.Get(TestTag)
	if len(nodes) == 0 {
		t.Fatal("get nodes failed")
	}
	if _, ok := nodes[TestAgent]; !ok {
		t.Fatal("get nodes failed")
	}

	// down node
	down()
	time.Sleep(3 * time.Second)

	// check again
	nodes = am.Get(TestTag)
	if len(nodes) != 0 {
		t.Fatal("get nodes failed")
	}
}
