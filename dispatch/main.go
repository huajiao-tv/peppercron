package dispatch

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

var (
	// AgentManager Agents manager
	AgentManager Agents

	// JobExecutions Job execution results
	JobExecutions Execution

	// JobDispatchers Job dispatcher manager
	JobDispatchers *JobDispatcherManager
)

var (
	leaderNode atomic.Value

	dispatchCtx       context.Context
	dispatchCtxCancel context.CancelFunc
)

// Leader 原子获取 Leader 节点
func Leader() string {
	if leader, ok := leaderNode.Load().(string); ok {
		return leader
	}
	return ""
}

func observe(ctx context.Context, el *concurrency.Election) {
	ch := el.Observe(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-ch:
			if len(resp.Kvs) == 0 {
				continue
			}
			oldLeader := Leader()
			leader := string(resp.Kvs[0].Value)
			leaderNode.Store(leader)
			// 调用停止方法，重新进入选举
			if oldLeader != leader && oldLeader == config.NodeID {
				util.Log.Trace("dispatch", "leader change", oldLeader, leader)
				dispatchCtxCancel()
			}
		}
	}
}

// Run 运行 Dispatch 实例
// Dispatch 只能有一个主，需要先选举
func Run(pctx context.Context) {
	var s *concurrency.Session
	s, err := concurrency.NewSession(config.Storage.Client)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// 选举逻辑
	el := concurrency.NewElection(s, logic.DispatchElectionMaster)

	// Dispatch Context 独立控制
	dispatchCtx, dispatchCtxCancel = context.WithCancel(pctx)

	// 实时获取当前谁是 Leader
	go observe(pctx, el)

	// 进入选举阻塞
	for {
		select {
		case <-pctx.Done():
			resignCtx, cancel := context.WithTimeout(pctx, 3*time.Second)
			err = el.Resign(resignCtx)
			if err != nil {
				panic(err)
			}
			cancel()
			return
		default:
			// Campaign will be blocked until win the election
			if err := el.Campaign(pctx, config.NodeID); err != nil {
				panic(err)
			}

			// 成功成为 Leader
			util.Log.Trace("dispatch", "etcd Campaign", "become leader")

			var leader *clientv3.GetResponse
			leader, err = el.Leader(pctx)
			if err != nil {
				panic(err)
			}

			// Recreate the election
			el = concurrency.ResumeElection(s, logic.DispatchElectionMaster,
				string(leader.Kvs[0].Key), leader.Kvs[0].CreateRevision)

			JobExecutions = NewJobExecution(dispatchCtx)

			// create dispatcher manager
			JobDispatchers = NewJobDispatcherManager(dispatchCtx)

			// monitor agent nodes
			AgentManager = NewAgentMonitor(dispatchCtx, JobDispatchers)

			// serve jobs dispatcher
			JobDispatchers.Serve()

			// Dispatcher 被停止后，重新生成新的 context 对象
			dispatchCtx, dispatchCtxCancel = context.WithCancel(pctx)

			resignCtx, cancel := context.WithTimeout(pctx, 3*time.Second)
			err = el.Resign(resignCtx)
			if err != nil {
				panic(err)
			}
			cancel()
		}
	}
}
