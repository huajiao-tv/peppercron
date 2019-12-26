package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
	v3 "go.etcd.io/etcd/clientv3"
)

const (
	// ETCDFailedRetryInterval  ETCD 失败重试间隔
	ETCDFailedRetryInterval = 1 * time.Second
)

var (
	ctx    context.Context
	cancel context.CancelFunc

	// JobsManager 具体的 JobManager 实例
	JobsManager *JobManager
)

// Run 为 Agent 启动方法
// 将启动 Agent 与 Job Manager 开始工作
func Run(pctx context.Context) {
	ctx, cancel = context.WithCancel(pctx)

	agent := NewAgent(ctx)
	go agent.KeepAlive()

	JobsManager = NewJobManager(ctx)
	if err := JobsManager.Serve(); err != nil {
		panic("Init Job Manager failed: " + err.Error())
	}
}

// Stop 降停止所有 Agent 服务
func Stop() {
	cancel()
}

// Agent 获取 Job 配置，根据 Job 规则运行对应的 Job
type Agent struct {
	ctx  context.Context
	tags string
}

// NewAgent 新建一个 Agent 实例
func NewAgent(ctx context.Context) *Agent {
	return &Agent{
		ctx: ctx,
	}
}

// KeepAlive 定时发送心跳包和自身的 Tag 到 etcd
func (agent *Agent) KeepAlive() {
	for {
		key := fmt.Sprintf(logic.AgentNodeAlive, config.NodeID)

		lease, err := config.Storage.Grant(agent.ctx, config.AgentHeartbeat*config.AgentHeartbeatTimes)
		if err != nil {
			util.Log.Error("agent", "KeepAlive", "get lease failed", err)
			time.Sleep(ETCDFailedRetryInterval)
			continue
		}

		setAgentTags := func(force bool) error {
			if force || agent.tags != config.RemoteConf().Tags {
				util.Log.Debug("agent", "KeepAlive", "Reset agent tags", force, agent.tags, config.RemoteConf().Tags)
				_, err = config.Storage.Put(agent.ctx, key, config.RemoteConf().Tags, v3.WithLease(lease.ID))
				if err != nil {
					return err
				}
				agent.tags = config.RemoteConf().Tags
			}

			return nil
		}

		setAgentTags(true)

		keepAliveOnce := func() error {
			_, err = config.Storage.KeepAliveOnce(agent.ctx, lease.ID)
			if err != nil {
				return err
			}

			return nil
		}

		ticker := time.NewTicker(config.AgentHeartbeat * time.Second)
		for {
			if err := setAgentTags(false); err != nil {
				util.Log.Error("agent", "KeepAlive", "put tags failed", err)
			}
			if err := keepAliveOnce(); err != nil {
				util.Log.Error("agent", "KeepAlive", "KeepAliveOnce failed", err)
				break
			}

			select {
			case <-agent.ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
			}
		}
		ticker.Stop()
	}

}
