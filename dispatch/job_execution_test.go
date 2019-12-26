package dispatch

import (
	"context"
	"testing"
	"time"

	"github.com/huajiao-tv/peppercron/logic"
)

func TestJobExecution(t *testing.T) {
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	exec := NewJobExecution(ctx)

	// subscribe
	td := &TestDispatcher{
		jobName:     "TestJobExecution",
		concurrency: 1,
		depRes:      make(map[string]*JobExecutionResult),
	}
	exec.Subscribe(td, []string{"exec_dep_1", "exec_dep_2"})

	// test notify
	result := &JobExecutionResult{
		ExecutionResult: logic.ExecutionResult{
			JobName: "exec_dep_1",
			GroupID: time.Now().UnixNano(),
		},
	}
	exec.JobCompleted(result)

	// check
	time.Sleep(time.Second)
	res := td.GetDepResult("exec_dep_1")
	if res == nil || res != result {
		t.Fatal("invalid dependent result")
	}

	// err test
	result.JobName = "exec_dep_0"
	exec.JobCompleted(result)

	// check
	time.Sleep(time.Second)
	if td.GetDepResult("exec_dep_0") != nil {
		t.Fatal("invalid depedent result")
	}
	res = td.GetDepResult("exec_dep_1")
	if res == nil {
		t.Fatal("invalid dependent result")
	}
}
