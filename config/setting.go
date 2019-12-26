package config

import (
	"context"
	"flag"
	"strings"
	"sync/atomic"

	"github.com/huajiao-tv/peppercron/backend"
)

const (
	// AgentHeartbeat 为 Agent 发送心跳的间隔
	AgentHeartbeat = 10
	// AgentHeartbeatTimes 为了防止心跳一次没法成功就导致 ETCD 认为 Agent 断线，设置 TTL 时，将心跳时间 * 心跳次数
	AgentHeartbeatTimes = 3
)

var (
	NodeID  string
	Storage *backend.Storage

	setting atomic.Value
)

var RemoteConf = func() *Setting {
	return setting.Load().(*Setting)
}

func UpdateConf(conf *Setting) {
	setting.Store(conf)
}

func Init(ctx context.Context) error {
	var (
		endPoints, user, password string
	)
	flag.StringVar(&endPoints, "e", "", "etcd end points")
	flag.StringVar(&user, "u", "", "etcd user name")
	flag.StringVar(&password, "p", "", "etcd user password")
	flag.StringVar(&NodeID, "n", "", "current node id")
	flag.Parse()

	s, err := backend.NewStorage(strings.Split(endPoints, ","), user, password)
	if err != nil {
		return err
	}
	Storage = s

	// get config
	if err = GetRemoteConfig(ctx); err != nil {
		return err
	}

	return nil
}
