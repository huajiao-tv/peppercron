package agent

import (
	"context"
	"testing"
	"time"

	"github.com/huajiao-tv/peppercron/logic"
)

type jobCallbackTest struct {
	name     string
	stopName string
	runTimes int64

	result  *logic.AgentExecutionResult
	storage JobResultStorage
}

func (t *jobCallbackTest) jobDone(config *logic.AgentJobConfig, ret *logic.AgentExecutionResult) {
	t.name = ret.JobName
	t.runTimes++
	t.result = ret
}

func (t *jobCallbackTest) jobStop(jobName string) {
	t.stopName = jobName
}

func TestJob(t *testing.T) {
	jobConf := &logic.AgentJobConfig{
		Name:            "Test",
		Type:            logic.JobType(logic.JobTypeSchedule),
		Times:           0,
		Schedule:        "@every 5s",
		ExecutorType:    logic.ExecutorType(logic.ExecutorTypeShell),
		ExecutorParams:  []string{"echo", "1"},
		ExecutorTimeout: 3 * time.Second,
	}

	jobCallbackTest := &jobCallbackTest{}
	job, err := NewJob(context.Background(), jobConf, jobCallbackTest)
	if err != nil {
		t.Error("job new error", err.Error())
	}

	go func() {
		if err := job.ScheduleRun(); err != nil {
			t.Error("job run error", err.Error())
		}
	}()
	time.Sleep(10 * time.Second)
	job.Stop()
	if jobCallbackTest.name != "Test" {
		t.Error("job callback error")
	}
	if jobCallbackTest.stopName != "Test" {
		t.Error("job stop callback error")
	}
}

// 无限次数的任务测试
func TestJobUnlimitedTimes(t *testing.T) {
	jobConf := &logic.AgentJobConfig{
		Name:            "Test",
		Type:            logic.JobType(logic.JobTypeTimes),
		Times:           -1,
		Schedule:        "",
		ExecutorType:    logic.ExecutorType(logic.ExecutorTypeShell),
		ExecutorParams:  []string{"sleep", "1"},
		ExecutorTimeout: 3 * time.Second,
	}

	jobCallbackTest := &jobCallbackTest{}
	job, err := NewJob(context.Background(), jobConf, jobCallbackTest)
	if err != nil {
		t.Error("job new error", err.Error())
	}
	go func() {
		if err := job.ScheduleRun(); err != nil {
			t.Error("job run error", err.Error())
		}
	}()
	time.Sleep(6 * time.Second)
	job.Stop()
	if jobCallbackTest.runTimes != 5 {
		t.Error("job run times wrong!", jobCallbackTest.runTimes)
	}
}

// 阻塞测试
// 执行间隔 0 - 1 - 2 - 1 - 2
// 执行时间 0 - 1 - 3 - 4 - 7
// 在第三秒和第七秒的时候会执行，并 runTimes++
// 所以 Sleep 8 秒后判断结果是否为 2
func TestJobRunWithBlocking(t *testing.T) {
	jobConf := &logic.AgentJobConfig{
		Name:             "Test",
		Type:             logic.JobType(logic.JobTypeSchedule),
		Schedule:         "@every 1s",
		ExecutorBlocking: true,
		ExecutorType:     logic.ExecutorType(logic.ExecutorTypeShell),
		ExecutorParams:   []string{"sleep", "2"},
		ExecutorTimeout:  10 * time.Second,
	}

	jobCallbackTest := &jobCallbackTest{}
	job, err := NewJob(context.Background(), jobConf, jobCallbackTest)
	if err != nil {
		t.Error("job new error", err.Error())
	}
	go func() {
		if err := job.ScheduleRun(); err != nil {
			t.Error("job run error", err.Error())
		}
	}()
	time.Sleep(8 * time.Second)
	job.Stop()
	if jobCallbackTest.runTimes != 2 {
		t.Error("job run with blocking failed, need 2, got", jobCallbackTest.runTimes)
	}
}
