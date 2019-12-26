package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
)

func TestAgentExecution(t *testing.T) {
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	dispatcher := &TestDispatcher{
		jobName:     "TestAgentExecution",
		concurrency: 1,
		depRes:      make(map[string]*JobExecutionResult),
	}
	exec := NewAgentExecution(ctx, dispatcher)
	go exec.Serve()

	// set agent result
	res := &logic.AgentExecutionResult{
		ExecutionResult: logic.ExecutionResult{
			JobName: dispatcher.Name(),
			GroupID: time.Now().UnixNano(),
		},
		AgentNode: TestAgent,
	}
	v, err := json.Marshal(res)
	if err != nil {
		t.Fatal("json marshal failed", err)
	}
	agentKey := fmt.Sprintf(logic.AgentExecutionRecord, dispatcher.Name(), TestAgent, res.GroupID)
	_, err = config.Storage.Put(ctx, agentKey, string(v))
	if err != nil {
		t.Fatal("put agent1 result failed", err)
	}

	// check job result
	time.Sleep(time.Second)
	jobRes := dispatcher.GetJobResult()
	if jobRes == nil || jobRes.GroupID != res.GroupID {
		t.Fatal("agent execution failed")
	}
}

func TestAgentConcurrencyExecution(t *testing.T) {
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	dispatcher := &TestDispatcher{
		jobName:     "TestAgentConcurrencyExecution",
		concurrency: 2,
	}
	exec := NewAgentExecution(ctx, dispatcher)
	go exec.Serve()

	// set agent result1
	res := &logic.AgentExecutionResult{
		ExecutionResult: logic.ExecutionResult{
			JobName: dispatcher.Name(),
			GroupID: time.Now().UnixNano(),
		},
		AgentNode: TestAgent,
	}
	v, err := json.Marshal(res)
	if err != nil {
		t.Fatal("json marshal failed", err)
	}
	agentKey1 := fmt.Sprintf(logic.AgentExecutionRecord, dispatcher.Name(), TestAgent, res.GroupID)
	_, err = config.Storage.Put(ctx, agentKey1, string(v))
	if err != nil {
		t.Fatal("put agent1 result failed", err)
	}

	// check job result
	time.Sleep(time.Second)
	jobRes := dispatcher.GetJobResult()
	if jobRes != nil {
		t.Fatal("agent execution invalid")
	}

	// set agent result2
	res.AgentNode = TestAgent2
	v, err = json.Marshal(res)
	if err != nil {
		t.Fatal("json marshal failed", err)
	}
	agentKey2 := fmt.Sprintf(logic.AgentExecutionRecord, dispatcher.Name(), TestAgent, res.GroupID)
	_, err = config.Storage.Put(ctx, agentKey2, string(v))
	if err != nil {
		t.Fatal("put agent1 result failed", err)
	}

	// check job result
	time.Sleep(time.Second)
	jobRes = dispatcher.GetJobResult()
	if jobRes == nil || jobRes.GroupID != res.GroupID {
		t.Fatal("agent execution failed")
	}
}
