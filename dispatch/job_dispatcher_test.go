package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	v3 "go.etcd.io/etcd/clientv3"
)

type TestAgents struct {
	downNode map[string]string
}

func (a *TestAgents) Get(tag string) map[string]string {
	ret := map[string]string{}
	switch tag {
	case TestTag:
		ret = map[string]string{
			TestAgent: "",
		}
	case TestConcurrencyTag:
		ret = map[string]string{
			TestAgent:  "",
			TestAgent2: "",
		}
	case TestNodeDownTag:
		ret = map[string]string{
			TestAgent:  "",
			TestAgent2: "",
			TestAgent3: "",
		}
	}
	for node := range a.downNode {
		delete(ret, node)
	}
	return ret
}

func (a *TestAgents) Check(node string) bool {
	switch node {
	case TestAgent, TestAgent2:
		return true
	default:
		return false
	}
}

func (a *TestAgents) Down(node string) {
	a.downNode[node] = ""
}

type TestExecution struct {
}

func (exe *TestExecution) GetMaxGroupIds([]string) map[string]int64 {
	return map[string]int64{}
}

func (exe *TestExecution) JobCompleted(res *JobExecutionResult) {
}

func (exe *TestExecution) Subscribe(Dispatcher, []string) {
}

func (exe *TestExecution) Unsubscribe(Dispatcher, []string) {
}

func (exe *TestExecution) JobDeleted(name string) {
}

func init() {
	AgentManager = &TestAgents{
		downNode: make(map[string]string),
	}
	JobExecutions = &TestExecution{}
}

func TestJobDispatch(t *testing.T) {
	job := logic.JobConfig{
		AgentJobConfig: logic.AgentJobConfig{
			DispatchID: "TestJobDispatch",
			Name:       "TestJobDispatch",
		},
		Tag:           TestTag,
		Concurrency:   1,
		DependentJobs: []string{},
	}
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	jd := NewJobDispatcher(ctx, &job)
	// start dispatcher
	go jd.Serve()

	// check dispatch results
	time.Sleep(time.Second)
	key := fmt.Sprintf(logic.JobDispatchAgent, TestAgent, job.Name)
	resp, err := config.Storage.Get(ctx, key)
	if err != nil {
		t.Fatal("get job failed", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("no job")
	}
}

func TestJobConcurrencyDispatch(t *testing.T) {
	job := logic.JobConfig{
		AgentJobConfig: logic.AgentJobConfig{
			DispatchID: "TestJobConcurrencyDispatch",
			Name:       "TestJobConcurrencyDispatch",
		},
		Tag:           TestConcurrencyTag,
		Concurrency:   7,
		DependentJobs: []string{},
	}
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	jd := NewJobDispatcher(ctx, &job)
	// start dispatcher
	go jd.Serve()

	// check dispatch results
	time.Sleep(time.Second)
	key1 := fmt.Sprintf(logic.JobDispatchAgent, TestAgent, job.Name)
	resp, err := config.Storage.Get(ctx, key1)
	if err != nil {
		t.Fatal("get job1 failed", err)
	}
	if len(resp.Kvs) != 4 {
		t.Fatal("no job1")
	}
	key2 := fmt.Sprintf(logic.JobDispatchAgent, TestAgent2, job.Name)
	resp, err = config.Storage.Get(ctx, key2)
	if err != nil {
		t.Fatal("get job2 failed", err)
	}
	if len(resp.Kvs) != 3 {
		t.Fatal("no job2")
	}
}

func TestJobDispatchWithDependent(t *testing.T) {
	job := logic.JobConfig{
		AgentJobConfig: logic.AgentJobConfig{
			Name: "TestJobDispatchWithDependent",
		},
		Tag:           TestTag,
		Concurrency:   1,
		DependentJobs: []string{"test_dep_1", "test_dep_2"},
	}
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	jd := NewJobDispatcher(ctx, &job)
	// start dispatcher
	go jd.Serve()

	// check dispatch results
	time.Sleep(time.Second)
	key := fmt.Sprintf(logic.JobDispatchAgent, TestAgent, job.Name)
	resp, err := config.Storage.Get(ctx, key)
	if err != nil {
		t.Fatal("get job failed", err)
	}
	if len(resp.Kvs) > 0 {
		t.Fatal("invalid job")
	}

	// update dependent 1
	result := &JobExecutionResult{
		ExecutionResult: logic.ExecutionResult{
			JobName: job.DependentJobs[0],
			GroupID: time.Now().UnixNano(),
		},
	}
	jd.DependencyCompleted(result)

	// check dispatch results
	time.Sleep(time.Second)
	resp, err = config.Storage.Get(ctx, key)
	if err != nil {
		t.Fatal("get job failed", err)
	}
	if len(resp.Kvs) > 0 {
		t.Fatal("invalid job")
	}

	// update dependent 2
	result.JobName = job.DependentJobs[1]
	jd.DependencyCompleted(result)

	// check dispatch results
	time.Sleep(time.Second)
	resp, err = config.Storage.Get(ctx, key)
	if err != nil {
		t.Fatal("get job failed", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("no job")
	}
}

func TestJobResult(t *testing.T) {
	job := logic.JobConfig{
		AgentJobConfig: logic.AgentJobConfig{
			Name: "TestJobResult",
		},
		Tag:           TestTag,
		Concurrency:   1,
		DependentJobs: []string{},
	}
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	jd := NewJobDispatcher(ctx, &job)
	// start dispatcher
	go jd.Serve()

	// check job result
	time.Sleep(time.Second)
	jobKey := fmt.Sprintf(logic.JobExecutionRecordPrefix, job.Name)
	resp, err := config.Storage.Get(ctx, jobKey, v3.WithPrefix())
	if err != nil {
		t.Fatal("get job result failed", err)
	}
	if len(resp.Kvs) > 0 {
		t.Fatal("invalid result")
	}

	// set agent result
	res := &logic.AgentExecutionResult{
		ExecutionResult: logic.ExecutionResult{
			JobName: job.Name,
			GroupID: time.Now().UnixNano(),
		},
		AgentNode: TestAgent,
	}
	v, err := json.Marshal(res)
	if err != nil {
		t.Fatal("json marshal failed", err)
	}
	agentKey := fmt.Sprintf(logic.AgentExecutionRecord, job.Name, TestAgent, res.GroupID)
	_, err = config.Storage.Put(ctx, agentKey, string(v))
	if err != nil {
		t.Fatal("put agent result failed", err)
	}

	// check job result
	time.Sleep(time.Second)
	resp, err = config.Storage.Get(ctx, jobKey, v3.WithPrefix())
	if err != nil {
		t.Fatal("get job result failed", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("no job result")
	}
}

func TestJobConcurrencyResult(t *testing.T) {
	job := logic.JobConfig{
		AgentJobConfig: logic.AgentJobConfig{
			Name: "TestJobConcurrencyResult",
		},
		Tag:           TestConcurrencyTag,
		Concurrency:   2,
		DependentJobs: []string{},
	}
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	jd := NewJobDispatcher(ctx, &job)
	// start dispatcher
	go jd.Serve()

	// check job result
	time.Sleep(time.Second)
	jobKey := fmt.Sprintf(logic.JobExecutionRecordPrefix, job.Name)
	resp, err := config.Storage.Get(ctx, jobKey, v3.WithPrefix())
	if err != nil {
		t.Fatal("get job result failed", err)
	}
	if len(resp.Kvs) > 0 {
		t.Fatal("invalid job result")
	}

	// set agent result
	res := &logic.AgentExecutionResult{
		ExecutionResult: logic.ExecutionResult{
			JobName: job.Name,
			GroupID: time.Now().UnixNano(),
		},
		AgentNode: TestAgent,
	}
	v, err := json.Marshal(res)
	if err != nil {
		t.Fatal("json marshal failed", err)
	}
	agentKey1 := fmt.Sprintf(logic.AgentExecutionRecord, job.Name, TestAgent, res.GroupID)
	_, err = config.Storage.Put(ctx, agentKey1, string(v))
	if err != nil {
		t.Fatal("put agent1 result failed", err)
	}

	// check job result
	time.Sleep(time.Second)
	resp, err = config.Storage.Get(ctx, jobKey, v3.WithPrefix())
	if err != nil {
		t.Fatal("get job result failed", err)
	}
	if len(resp.Kvs) > 0 {
		t.Fatal("invalid job result")
	}

	// set agent2 result
	res.AgentNode = TestAgent2
	v, err = json.Marshal(res)
	if err != nil {
		t.Fatal("json marshal failed", err)
	}
	agentKey2 := fmt.Sprintf(logic.AgentExecutionRecord, job.Name, TestAgent2, res.GroupID)
	_, err = config.Storage.Put(ctx, agentKey2, string(v))
	if err != nil {
		t.Fatal("put agent2 result failed", err)
	}

	// check job result
	time.Sleep(time.Second)
	resp, err = config.Storage.Get(ctx, jobKey, v3.WithPrefix())
	if err != nil {
		t.Fatal("get job result failed", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("no job result")
	}
}

func TestJobDispatcher_AgentDown(t *testing.T) {
	job := logic.JobConfig{
		AgentJobConfig: logic.AgentJobConfig{
			Name: "TestJobDispatcher_AgentDown",
		},
		Tag:           TestNodeDownTag,
		Concurrency:   2,
		DependentJobs: []string{},
	}
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	jd := NewJobDispatcher(ctx, &job)
	// start dispatcher
	go jd.Serve()

	// check dispatch results
	assigned := 0
	freeNode := ""
	time.Sleep(time.Second)
	for node := range AgentManager.Get(TestNodeDownTag) {
		key := fmt.Sprintf(logic.JobDispatchAgent, node, job.Name)
		resp, err := config.Storage.Get(ctx, key)
		if err != nil {
			t.Fatal("get job1 failed", err)
		}
		if len(resp.Kvs) > 0 {
			assigned++
		} else {
			freeNode = TestAgent
		}
	}
	if assigned < job.Concurrency {
		t.Fatal("dispatch job failed")
	}

	// down node
	downNode := ""
	for node := range AgentManager.Get(TestNodeDownTag) {
		if node != freeNode {
			downNode = node
			AgentManager.(*TestAgents).Down(node)
			jd.AgentDown(node)
			break
		}
	}

	// check again
	time.Sleep(time.Second)
	upKey := fmt.Sprintf(logic.JobDispatchAgent, freeNode, job.Name)
	resp, err := config.Storage.Get(ctx, upKey)
	if err != nil {
		t.Fatal("get job1 failed", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("dispatch failed")
	}
	keyDown := fmt.Sprintf(logic.JobDispatchAgent, downNode, job.Name)
	resp, err = config.Storage.Get(ctx, keyDown)
	if err != nil {
		t.Fatal("get job1 failed", err)
	}
	if len(resp.Kvs) > 0 {
		t.Fatal("down node still has job")
	}
}
