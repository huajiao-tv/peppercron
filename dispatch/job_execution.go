package dispatch

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
	v3 "go.etcd.io/etcd/clientv3"
)

type JobExecutionResult struct {
	logic.ExecutionResult

	DispatchIDs []string `json:"dispatch_ids"`

	// Number of successful executions of this job.
	SuccessCount int `json:"success_count"`

	// Number of errors running this job.
	ErrorCount int `json:"error_count"`

	// Last time this job executed successful.
	LastSuccess time.Time `json:"last_success"`

	// Last time this job failed.
	LastError time.Time `json:"last_error"`
}

type Execution interface {
	// Get jobs execution group id
	GetMaxGroupIds([]string) map[string]int64
	// Job complete notify
	JobCompleted(*JobExecutionResult)

	JobDeleted(string)

	// Subscribe/Unsubscribe job complete events
	Subscribe(Dispatcher, []string)
	Unsubscribe(Dispatcher, []string)
}

type JobExecution struct {
	sync.RWMutex
	subscribes map[string]map[string]Dispatcher
	results    map[string]map[int64]*JobExecutionResult
	maxGroup   map[string]int64

	ctx        context.Context
	notifyChan chan *JobExecutionResult
}

func NewJobExecution(pctx context.Context) Execution {
	jobExec := &JobExecution{
		ctx:        pctx,
		subscribes: make(map[string]map[string]Dispatcher),
		results:    make(map[string]map[int64]*JobExecutionResult),
		maxGroup:   make(map[string]int64),
		notifyChan: make(chan *JobExecutionResult, 1000),
	}

	// recover from etcd
	jobExec.recover()

	// notify
	go jobExec.serve()

	return jobExec
}

func (jobExec *JobExecution) GetMaxGroupIds(names []string) map[string]int64 {
	res := make(map[string]int64, len(names))
	jobExec.RLock()
	for _, name := range names {
		res[name] = jobExec.maxGroup[name]
	}
	jobExec.RUnlock()
	return res
}

func (jobExec *JobExecution) JobCompleted(res *JobExecutionResult) {
	jobExec.notifyChan <- res
}

func (jobExec *JobExecution) Subscribe(jobDisp Dispatcher, dependents []string) {
	jobExec.Lock()
	for _, name := range dependents {
		subs, ok := jobExec.subscribes[name]
		if !ok {
			subs = make(map[string]Dispatcher)
			jobExec.subscribes[name] = subs
		}
		subs[jobDisp.Name()] = jobDisp
	}
	jobExec.Unlock()
}

func (jobExec *JobExecution) Unsubscribe(jobDisp Dispatcher, dependents []string) {
	jobExec.Lock()
	for _, name := range dependents {
		subs, ok := jobExec.subscribes[name]
		if ok {
			delete(subs, jobDisp.Name())
		}
	}
	jobExec.Unlock()
}

func (jobExec *JobExecution) serve() {
	for {
		select {

		case <-jobExec.ctx.Done():
			return

		case res := <-jobExec.notifyChan:
			jobExec.RLock()
			if subs, ok := jobExec.subscribes[res.JobName]; ok {
				util.Log.Debug("JobExecution", "JobCompleted has subs", subs)
				for _, jd := range subs {
					jd.DependencyCompleted(res)
				}
			}
			jobExec.RUnlock()
		}
	}
}

func (jobExec *JobExecution) recover() {
	resp, err := config.Storage.Get(jobExec.ctx, logic.BaseJobExecutionRecordPrefix, v3.WithPrefix())
	if err != nil {
		util.Log.Error("JobExecution", "recover", "load base job exec record failed", err)
		return
	}

	for _, kv := range resp.Kvs {
		res := JobExecutionResult{}
		if err := json.Unmarshal(kv.Value, &res); err != nil {
			util.Log.Error("JobExecution", "recover", "job exec result json decode failed", err)
			continue
		}
		// update results, DO NOT need to send notification
		jobExec.update(&res)
	}
}

func (jobExec *JobExecution) update(res *JobExecutionResult) bool {
	jobExec.Lock()
	defer jobExec.Unlock()
	// update results
	jobRes, ok := jobExec.results[res.JobName]
	if !ok {
		jobRes = make(map[int64]*JobExecutionResult)
		jobExec.results[res.JobName] = jobRes
	}
	jobRes[res.GroupID] = res

	// update max group id
	if res.GroupID > jobExec.maxGroup[res.JobName] {
		jobExec.maxGroup[res.JobName] = res.GroupID
		return true
	}
	return false
}

func (jobExec *JobExecution) JobDeleted(jobName string) {
	jobExec.Lock()
	defer jobExec.Unlock()

	delete(jobExec.results, jobName)
	delete(jobExec.subscribes, jobName)
	delete(jobExec.maxGroup, jobName)
}
