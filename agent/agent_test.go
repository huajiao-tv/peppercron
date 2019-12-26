package agent

import (
	"context"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/huajiao-tv/peppercron/backend"
	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	v3 "go.etcd.io/etcd/clientv3"
)

func init() {
	var endPoints, user, password string
	flag.StringVar(&endPoints, "e", "", "etcd end points")
	flag.StringVar(&user, "u", "", "etcd user name")
	flag.StringVar(&password, "p", "", "etcd user password")
	flag.Parse()

	config.Storage, _ = backend.NewStorage(strings.Split(endPoints, ","), user, password)
	config.Storage.Delete(context.TODO(), logic.KeyPrefix, v3.WithPrefix())
}

func TestAgentAlive(t *testing.T) {
	// set config
	config.NodeID = "TestAgentAlive"
	config.UpdateConf(&config.Setting{
		SettingNode: config.SettingNode{
			Tags: "Tag_TestAgentAlive",
		},
	})

	// create instance
	ctx, stop := context.WithCancel(context.Background())
	agent := NewAgent(ctx)
	// start agent
	go agent.KeepAlive()

	// check agent state
	time.Sleep(time.Second)
	resp, err := config.Storage.Get(ctx, logic.AgentNodeAlivePrefix, v3.WithPrefix())
	if err != nil {
		t.Fatal("get agent node failed", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("empty agent node")
	}

	// stop
	stop()

	// check agent
	time.Sleep(config.AgentHeartbeat*config.AgentHeartbeatTimes + 1*time.Second)
	resp, err = config.Storage.Get(context.TODO(), logic.AgentNodeAlivePrefix, v3.WithPrefix())
	if err != nil {
		t.Fatal("get agent node failed", err)
	}
	if len(resp.Kvs) > 0 {
		t.Fatal("invalid node")
	}
}
