package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
	v3 "go.etcd.io/etcd/clientv3"
	pb "go.etcd.io/etcd/mvcc/mvccpb"
)

// AgentExectuion 执行结果处理
type AgentExecution struct {
	ctx context.Context
	jd  Dispatcher

	agentRes map[int64]map[string]*logic.AgentExecutionResult
}

// NewAgentExecution 新建执行结果处理程序
// 每个 JobDispatcher 一个
func NewAgentExecution(pctx context.Context, jd Dispatcher) *AgentExecution {
	return &AgentExecution{
		ctx: pctx,
		jd:  jd,

		agentRes: make(map[int64]map[string]*logic.AgentExecutionResult),
	}
}

// Serve 开始处理服务
// Watch 新执行结果
func (agentExec *AgentExecution) Serve() {
ReWatch:
	key := fmt.Sprintf(logic.BaseAgentExecutionRecordPrefix, agentExec.jd.Name()) + ":"
	agentChs := config.Storage.Watch(agentExec.ctx, key, v3.WithPrefix())

	// recover agent results
	agentExec.recover()
	// check agent results
	agentExec.checkResult(0)

	for {
		select {

		case <-agentExec.ctx.Done():
			return

		case resp := <-agentChs:
			if resp.Err() != nil {
				util.Log.Error("AgentExectuion", "Serve", "watch failed", resp.Err().Error())
				time.Sleep(200 * time.Millisecond)
				goto ReWatch
			}
			for _, evt := range resp.Events {
				if evt.Type != pb.PUT {
					continue
				}

				ret := logic.AgentExecutionResult{}
				if err := json.Unmarshal(evt.Kv.Value, &ret); err != nil {
					util.Log.Error("AgentExectuion", "Serve", "unknown event", err)
					continue
				}

				agentExec.addResult(&ret)
				// check agent results
				agentExec.checkResult(ret.GroupID)
			}
		}
	}
}

func (agentExec *AgentExecution) recover() {
	// agent execution results
	agentKey := fmt.Sprintf(logic.BaseAgentExecutionRecordPrefix, agentExec.jd.Name()) + ":"
	agentResp, err := config.Storage.Get(agentExec.ctx, agentKey, v3.WithPrefix())
	if err != nil {
		util.Log.Error("AgentExectuion", "recover", "get record failed", err)
		return
	}
	for _, kv := range agentResp.Kvs {
		ret := logic.AgentExecutionResult{}
		if err := json.Unmarshal(kv.Value, &ret); err != nil {
			util.Log.Error("AgentExectuion", "recover", "parse agent exec result failed", err)
			continue
		}
		if ret.JobName != agentExec.jd.Name() {
			continue
		}
		agentExec.addResult(&ret)
	}
}

func (agentExec *AgentExecution) addResult(res *logic.AgentExecutionResult) {
	// check concurrency results
	resultGroup, ok := agentExec.agentRes[res.GroupID]
	if !ok {
		resultGroup = make(map[string]*logic.AgentExecutionResult, agentExec.jd.Concurrency())
		agentExec.agentRes[res.GroupID] = resultGroup
	}
	resultGroup[res.DispatchID] = res
}

func (agentExec *AgentExecution) checkResult(groupId int64) {
	for gid, agentsMap := range agentExec.agentRes {
		if gid < groupId || len(agentsMap) < agentExec.jd.Concurrency() {
			continue
		}

		jobRes := JobExecutionResult{
			ExecutionResult: logic.ExecutionResult{
				JobName: agentExec.jd.Name(),
				GroupID: gid,
			},
		}
		for _, res := range agentsMap {
			jobRes.DispatchIDs = append(jobRes.DispatchIDs, res.DispatchID)

			if res.JobCompleted {
				jobRes.JobCompleted = true
			}

			if res.Status == logic.StatusOK {
				jobRes.SuccessCount++
				jobRes.LastSuccess = time.Now()
			} else {
				jobRes.ErrorCount++
				jobRes.LastError = time.Now()
			}
		}
		agentExec.jd.AgentsCompleted(&jobRes)
		delete(agentExec.agentRes, gid)
	}
}
