package dispatch

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
	v3 "go.etcd.io/etcd/clientv3"
	pb "go.etcd.io/etcd/mvcc/mvccpb"
)

type Agents interface {
	Get(tag string) map[string]string
	Check(agent string) bool
}

type AgentMonitor struct {
	sync.RWMutex
	tags  map[string]map[string]string
	nodes map[string]string

	cb  AgentUpdate
	ctx context.Context
}

func NewAgentMonitor(pctx context.Context, callback AgentUpdate) Agents {
	mgr := &AgentMonitor{
		ctx:   pctx,
		cb:    callback,
		tags:  make(map[string]map[string]string),
		nodes: make(map[string]string),
	}

	// recover
	mgr.recover()

	// monitor agents up/down
	go mgr.monitor(mgr.ctx)
	return mgr
}

func (m *AgentMonitor) Get(tag string) map[string]string {
	m.RLock()
	t, ok := m.tags[tag]
	m.RUnlock()
	if ok {
		return t
	}
	return map[string]string{}
}

func (m *AgentMonitor) Check(agent string) bool {
	m.RLock()
	_, ok := m.nodes[agent]
	m.RUnlock()
	return ok
}

func (m *AgentMonitor) recover() {
	resp, err := config.Storage.Get(m.ctx, logic.AgentNodeAlivePrefix, v3.WithPrefix())
	if err != nil {
		util.Log.Error("AgentMonitor", "recover", "get agent node alive status failed", err)
		return
	}
	for _, kv := range resp.Kvs {
		var node string
		n, err := fmt.Sscanf(string(kv.Key), logic.AgentNodeAlive, &node)
		if err != nil || n != 1 || node == "" {
			util.Log.Error("AgentMonitor", "recover", "invalid node")
			continue
		}
		util.Log.Debug("AgentMonitor", "recover", node)
		// TODO: 删除多余的机器，如果重新 watch 的时候也会用到
		m.addNode(node, strings.Split(string(kv.Value), ","))
	}
}

func (m *AgentMonitor) monitor(ctx context.Context) {
ReWatch:
	// subscribe with prefix and preKv
	ch := config.Storage.Watch(ctx, logic.AgentNodeAlivePrefix, v3.WithPrefix(), v3.WithPrevKV())
	// TODO: 增加恢复逻辑
	for {
		select {

		case <-m.ctx.Done():
			return

		case resp := <-ch:
			if resp.Err() != nil {
				util.Log.Error("AgentMonitor", "monitor", "watch failed", resp.Err().Error())
				time.Sleep(200 * time.Millisecond)
				goto ReWatch
			}
			for _, evt := range resp.Events {
				var (
					node string
				)
				n, err := fmt.Sscanf(string(evt.Kv.Key), logic.AgentNodeAlive, &node)
				if err != nil || n != 1 || node == "" {
					util.Log.Error("AgentMonitor", "recover", "invalid node")
					continue
				}

				switch evt.Type {
				case pb.PUT:
					util.Log.Debug("AgentMonitor", "monitor node up", node, string(evt.Kv.Value))
					m.nodeUp(node, string(evt.Kv.Value))
				case pb.DELETE:
					util.Log.Debug("AgentMonitor", "monitor node down", node, string(evt.PrevKv.Value))
					m.nodeDown(node, string(evt.PrevKv.Value))
				}
			}
		}
	}
}

func (m *AgentMonitor) addNode(node string, tags []string) {
	m.Lock()
	m.nodes[node] = ""
	for _, tag := range tags {
		tm, ok := m.tags[tag]
		if !ok {
			tm = make(map[string]string)
			m.tags[tag] = tm
		}
		// todo value?
		tm[node] = ""
	}
	m.Unlock()
}

func (m *AgentMonitor) nodeUp(node string, tagString string) {
	util.Log.Trace("AgentMonitor.nodeUp", node, tagString)
	tags := strings.Split(tagString, ",")
	if len(tags) == 0 {
		return
	}

	// add agent node
	m.addNode(node, tags)

	// callback
	if m.cb != nil {
		m.cb.AgentUp(node, tags)
	}
}

func (m *AgentMonitor) nodeDown(node string, tagString string) {
	util.Log.Trace("AgentMonitor.nodeDown", node, tagString)
	tags := strings.Split(tagString, ",")

	m.Lock()
	delete(m.nodes, node)
	for _, tag := range tags {
		tm, ok := m.tags[tag]
		if ok {
			delete(tm, node)
		}
	}
	m.Unlock()

	// callback
	if m.cb != nil {
		m.cb.AgentDown(node, tags)
	}
}
