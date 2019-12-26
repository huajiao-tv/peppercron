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
)

type Dispatcher interface {
	// agent node up/down notification
	AgentUp(string)
	AgentDown(string)

	// dispatcher properties Name() string
	Name() string
	Concurrency() int

	// operations
	Serve()
	Stop()

	// agent/job results notification
	DependencyCompleted(*JobExecutionResult)
	AgentsCompleted(*JobExecutionResult)
}

const (
	DependentExecutionEvent = iota
	JobExecutionEvent
	AgentDownEvent
	AgentUpEvent
)

type DispatcherEvent struct {
	EventType int
	EventData interface{}
}

type JobDispatchState struct {
	// job dependents state
	Dependents map[string]int64 `json:"dependents"`

	// job running agents state
	// map[node]map[dispatch id]ok
	RunningAgents map[string]map[string]bool `json:"running_agents"`

	// job all completed state
	// will not dispatch again
	JobCompleted bool `json:"job_completed"`
}

func (jds *JobDispatchState) RunningCount() int {
	runningCount := 0
	for _, agent := range jds.RunningAgents {
		for dispatchIDs := range agent {
			runningCount += len(dispatchIDs)
		}
	}
	return runningCount
}

func (jds *JobDispatchState) ToString() (string, error) {
	if v, err := json.Marshal(jds); err != nil {
		return "", err
	} else {
		return string(v), nil
	}
}

type JobDispatcher struct {
	state     *JobDispatchState
	eventChan chan *DispatcherEvent

	job         *logic.JobConfig
	execResults *AgentExecution

	ctx  context.Context
	stop context.CancelFunc
}

func NewJobDispatcher(ctx context.Context, job *logic.JobConfig) Dispatcher {
	// double check job config
	if len(job.DependentJobs) > 0 {
		job.Type = logic.JobTypeTimes
		job.Times = 1
	}
	jd := &JobDispatcher{
		job:       job,
		eventChan: make(chan *DispatcherEvent, 100),
	}
	jd.ctx, jd.stop = context.WithCancel(ctx)
	return jd
}

func (jd *JobDispatcher) Name() string {
	return jd.job.Name
}

func (jd *JobDispatcher) Concurrency() int {
	return jd.job.Concurrency
}

// stop job dispatcher
func (jd *JobDispatcher) Stop() {
	util.Log.Debug("JobDispatcher.Stop", jd.Name())

	select {
	case <-jd.ctx.Done():
	default:
		jd.stop()
	}
	// clean up resource
	keys := []string{
		fmt.Sprintf(logic.JobDispatchRecord, jd.job.Name),
	}
	for node, data := range jd.state.RunningAgents {
		for dispatchID := range data {
			keys = append(keys, fmt.Sprintf(logic.JobDispatchAgent, node, dispatchID))
		}
	}
	for _, key := range keys {
		config.Storage.Delete(context.Background(), key)
	}

	prefixKey := fmt.Sprintf(logic.JobExecutionRecordPrefix, jd.job.Name) + "/"
	config.Storage.Delete(context.Background(), prefixKey, v3.WithPrefix())

	JobExecutions.JobDeleted(jd.job.Name)
}

func (jd *JobDispatcher) Serve() {
	var (
		depResults map[string]int64
	)

	// load dispatch state
	jd.loadState()

	// 任务完全完成以后不需要运行
	// 直接退出
	if jd.state.JobCompleted {
		return
	}

	// create agent execution monitor
	jd.execResults = NewAgentExecution(jd.ctx, jd)

	// check agent job results
	go jd.execResults.Serve()

	// dispatch job
	if len(jd.job.DependentJobs) > 0 {
		// subscribe dependent jobs execution
		JobExecutions.Subscribe(jd, jd.job.DependentJobs)

		// get dependent jobs info and check
		depResults = JobExecutions.GetMaxGroupIds(jd.job.DependentJobs)
		if jd.checkDependents(depResults) {
			// dispatch once
			jd.dispatch(false)
		}
	} else {
		jd.dispatch(false)
	}

	for {
		select {

		case <-jd.ctx.Done():
			if len(jd.job.DependentJobs) > 0 {
				// unsubscribe
				JobExecutions.Unsubscribe(jd, jd.job.DependentJobs)
			}
			return

		case evt := <-jd.eventChan:
			switch evt.EventType {

			case AgentUpEvent:
				node := evt.EventData.(string)
				jd.onNodeUp(node)

			case AgentDownEvent:
				node := evt.EventData.(string)
				jd.onNodeDown(node)

			case JobExecutionEvent:
				res := evt.EventData.(*JobExecutionResult)
				jd.onJobCompleted(res)

			case DependentExecutionEvent:
				depRes := evt.EventData.(*JobExecutionResult)
				util.Log.Debug("JobDispatcher", "DependencyCompleted", depRes.JobName, jd.job.Name, jd.state.Dependents)
				depResults[depRes.JobName] = depRes.GroupID
				// check
				if jd.checkDependents(depResults) {
					// dispatch once
					jd.dispatch(false)
				}

			default:
				continue
			}
		}
	}
}

func (jd *JobDispatcher) AgentsCompleted(res *JobExecutionResult) {
	util.Log.Debug("JobDispatcher.AgentsCompleted",
		res.JobName, res.DispatchID, res.GroupID, res.SuccessCount, res.ErrorCount, res.JobCompleted, res.DispatchIDs)

	jd.eventChan <- &DispatcherEvent{
		EventType: JobExecutionEvent,
		EventData: res,
	}
}

func (jd *JobDispatcher) DependencyCompleted(res *JobExecutionResult) {
	util.Log.Debug("JobDispatcher.DependencyCompleted",
		res.JobName, res.DispatchID, res.GroupID)

	jd.eventChan <- &DispatcherEvent{
		EventType: DependentExecutionEvent,
		EventData: res,
	}
}

func (jd *JobDispatcher) AgentUp(node string) {
	jd.eventChan <- &DispatcherEvent{
		EventType: AgentUpEvent,
		EventData: node,
	}
}

func (jd *JobDispatcher) AgentDown(node string) {
	jd.eventChan <- &DispatcherEvent{
		EventType: AgentDownEvent,
		EventData: node,
	}
}

func (jd *JobDispatcher) onNodeUp(node string) {
	if len(jd.job.DependentJobs) > 0 {
		// If has dependent jobs, next dispatch will assign nodes
		return
	}
	util.Log.Debug("JobDispatcher", "onNodeUp", "Debug", jd.job.Name, node)
	if jd.state.RunningCount() >= jd.job.Concurrency {
		return
	}
	// assign jobs
	jd.assignJobToAgent(false)
	// update dispatch state to etcd
	if err := jd.putState(); err != nil {
		util.Log.Error("JobDispatcher", "onNodeUp", jd.job.Name, node, "add job to up node failed", err)
		return
	}
}

func (jd *JobDispatcher) onNodeDown(node string) {
	_, ok := jd.state.RunningAgents[node]
	if !ok {
		return
	}
	util.Log.Debug("JobDispatcher", "onNodeDown", "Debug", jd.job.Name, node)
	if len(jd.job.DependentJobs) > 0 {
		// If has dependent jobs, next dispatch will assign nodes
		return
	}
	// dispatch to other agents
	// delete job of down node
	jd.cleanNode(node)
	// assign jobs
	jd.assignJobToAgent(false)
	// update dispatch state to etcd
	if err := jd.putState(); err != nil {
		util.Log.Error("JobDispatcher", "onNodeDown", jd.job.Name, node, "delete job for down node failed", err)
		return
	}
}

func (jd *JobDispatcher) onJobCompleted(res *JobExecutionResult) {
	util.Log.Debug("JobDispatcher", "onJobCompleted",
		res.JobName, res.DispatchID, res.GroupID, res.SuccessCount, res.ErrorCount, res.JobCompleted)

	//  delete completed job
	if res.JobCompleted {
		jd.state.JobCompleted = true
		//  delete job state
		jd.cleanJob()
		jd.putState()
	}

	// store job execution result
	v, err := json.Marshal(res)
	if err != nil {
		util.Log.Error("JobDispatcher", "onJobCompleted", jd.job.Name, res.DispatchID, "store job exec result json encode failed", err)
		return
	}

	ttl := 24 * time.Hour
	ttlLease, err := config.Storage.Grant(context.TODO(), int64(ttl.Seconds()))
	if err != nil {
		util.Log.Error("agent", "jobDone", "set status: get lease failed", err)
		return
	}
	key := fmt.Sprintf(logic.JobExecutionRecord, res.JobName, res.GroupID)
	_, err = config.Storage.Put(jd.ctx, key, string(v), v3.WithLease(ttlLease.ID))
	if err != nil {
		util.Log.Error("JobDispatcher", "onJobCompleted", jd.job.Name, res.DispatchID, "store job exec result storage failed", err)
		return
	}

	util.Log.Debug("JobDispatcher", "onJobCompleted", jd.job.Name, res.DispatchID, res.ExecutionResult)

	// notify
	JobExecutions.JobCompleted(res)
}

func (jd *JobDispatcher) putState() error {
	// store dispatch state
	key := fmt.Sprintf(logic.JobDispatchRecord, jd.job.Name)
	v, err := json.Marshal(jd.state)
	if err != nil {
		return err
	}
	_, err = config.Storage.Put(jd.ctx, key, string(v))
	if err != nil {
		return err
	}
	return nil
}

func (jd *JobDispatcher) cleanJob() {
	for node, data := range jd.state.RunningAgents {
		for dispatchID := range data {
			key := fmt.Sprintf(logic.JobDispatchAgent, node, dispatchID)
			config.Storage.Delete(context.Background(), key)
		}
		delete(jd.state.RunningAgents, node)
	}
}

func (jd *JobDispatcher) cleanNode(deleteNode string) {
	if deleteNode == "" {
		return
	}
	for node := range jd.state.RunningAgents {
		if node != deleteNode {
			continue
		}

		key := fmt.Sprintf(logic.JobDispatchAgentPrefix, node) + "/"
		config.Storage.Delete(context.Background(), key, v3.WithPrefix())
	}
	delete(jd.state.RunningAgents, deleteNode)
}

// load job dispatch state
func (jd *JobDispatcher) loadState() {
	key := fmt.Sprintf(logic.JobDispatchRecord, jd.job.Name)
	resp, err := config.Storage.Get(jd.ctx, key)
	if err != nil {
		util.Log.Error("JobDispatcher", "loadState", jd.job.Name, "load state failed", err)
		goto Reset
	}
	if len(resp.Kvs) == 0 {
		goto Reset
	}
	if err := json.Unmarshal(resp.Kvs[0].Value, &jd.state); err != nil {
		util.Log.Error("JobDispatcher", "loadState", jd.job.Name, "state decode failed", err)
		goto Reset
	}

	if jd.state.JobCompleted {
		return
	}

	// check all agents are alive
	for node := range jd.state.RunningAgents {
		if !AgentManager.Check(node) {
			jd.cleanNode(node)
		}
	}
	// put state
	jd.putState()

	return

Reset:
	jd.state = &JobDispatchState{
		Dependents:    make(map[string]int64, len(jd.job.DependentJobs)),
		RunningAgents: make(map[string]map[string]bool, jd.job.Concurrency),
	}
	for _, name := range jd.job.DependentJobs {
		jd.state.Dependents[name] = 0
	}
}

func (jd *JobDispatcher) assignJobToAgent(force bool) {
	if jd.state.JobCompleted {
		util.Log.Warn("JobDispatcher", "assignAgents", jd.job.Name, "job completed")
		return
	}

	nodes := AgentManager.Get(jd.job.Tag)
	if len(nodes) == 0 {
		util.Log.Error("JobDispatcher", "assignAgents", "nodes is empty", jd.job.Name, jd.job.Tag)
		return
	}

	i := 0
	nodeIds := make(map[int]string)
	for node := range nodes {
		nodeIds[i] = node
		i++
	}

	baseGroupID := time.Now().UnixNano()

	// 分配逻辑处理，当机器够用时，并发均分到各个机器
	count := jd.job.Concurrency - jd.state.RunningCount()
	if force {
		count = jd.job.Concurrency
	}
	offset := 0
	for i := 1; i <= count; i++ {
		node := nodeIds[offset]
		offset++
		if i%len(nodes) == 0 {
			offset = 0
		}

		// assign job
		dispatchID := fmt.Sprintf("%s:%s:%v", jd.job.Name, node, time.Now().UnixNano())
		jd.job.AgentJobConfig.DispatchID = dispatchID
		jd.job.AgentJobConfig.BaseGroupID = baseGroupID

		if _, ok := jd.state.RunningAgents[node]; !ok {
			jd.state.RunningAgents[node] = make(map[string]bool)
		}
		jd.state.RunningAgents[node][dispatchID] = true

		key := fmt.Sprintf(logic.JobDispatchAgent, node, jd.job.DispatchID)
		agentJobConf, err := jd.job.AgentJobConfig.ToString()
		if err != nil {
			util.Log.Error("JobDispatcher", "assignJobs", jd.job.Name, "job to string failed", err)
			continue
		}
		_, err = config.Storage.Put(jd.ctx, key, agentJobConf)
		if err != nil {
			util.Log.Error("JobDispatcher", "assignJobs", jd.job.Name, "storage job failed", err)
			continue
		}
	}
}

func (jd *JobDispatcher) dispatch(force bool) {
	util.Log.Debug("JobDispatcher", "dispatch", jd.job.Name, force)
	jd.assignJobToAgent(force)

	// update dispatch state to etcd
	if err := jd.putState(); err != nil {
		util.Log.Error("JobDispatcher", "dispatch", jd.job.Name, "update state failed", err)
	}
}

func (jd *JobDispatcher) checkDependents(results map[string]int64) bool {
	completed := true
	for name, expected := range jd.state.Dependents {
		current, ok := results[name]
		if !ok || current <= expected {
			completed = false
			break
		}
	}
	// update dependents group id
	if completed {
		for name, gid := range results {
			jd.state.Dependents[name] = gid
		}
	}
	return completed
}
