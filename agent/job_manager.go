package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
	v3 "go.etcd.io/etcd/clientv3"
	pb "go.etcd.io/etcd/mvcc/mvccpb"
)

type jobCallback interface {
	jobDone(*logic.AgentJobConfig, *logic.AgentExecutionResult)
	jobStop(string)
}

// JobManager 是 Job 的管理者
// 负责从 etcd 获取 Job 配置，启动相对应的 Job，与 Job 执行结果回调
type JobManager struct {
	jobs map[string]*Job

	jobResultStorage      JobResultStorage
	jobResultTimesStorage JobResultStorage

	ctx context.Context
	sync.RWMutex
}

// NewJobManager 新建一个 Job 管理者
func NewJobManager(ctx context.Context) *JobManager {
	// 初始化 Job 结果存储
	var storage JobResultStorage
	var timesStorage JobResultStorage

	var err error
	if config.RemoteConf().JobResultStorage != nil {
		switch config.RemoteConf().JobResultStorage.Type {
		case "mysql":
			storage, err = NewJobResultMySQLStorage(
				ctx,
				config.RemoteConf().JobResultStorage.Addr,
				config.RemoteConf().JobResultStorage.User,
				config.RemoteConf().JobResultStorage.Auth,
				config.RemoteConf().JobResultStorage.Database,
			)
			if err != nil {
				util.Log.Error("NewJobManager", "init job result storage failed", err)
			}
		}
	}

	if config.RemoteConf().JobResultTimesStorage != nil {
		switch config.RemoteConf().JobResultTimesStorage.Type {
		case "redis":
			timesStorage, err = NewJobResultTimesRedisStorage(
				config.RemoteConf().JobResultTimesStorage.Addr,
				config.RemoteConf().JobResultTimesStorage.Auth,
				config.RemoteConf().JobResultTimesStorage.MaxConnNum,
				config.RemoteConf().JobResultTimesStorage.IdleTimeout,
			)
			if err != nil {
				util.Log.Error("NewJobManager", "init job times result storage failed", err)
			}
		}
	}

	mgr := &JobManager{
		jobs:                  make(map[string]*Job),
		jobResultStorage:      storage,
		jobResultTimesStorage: timesStorage,

		ctx: ctx,
	}
	return mgr
}

// load 读取 etcd 中的 Job 配置，循环启动 Job
func (jobManager *JobManager) load() error {
	key := fmt.Sprintf(logic.JobDispatchAgentPrefix, config.NodeID)
	resp, err := config.Storage.Get(jobManager.ctx, key, v3.WithPrefix())
	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		job := logic.AgentJobConfig{}
		if err = job.Parse(kv.Value); err != nil {
			util.Log.Error("JobManager", "init", "job.Parse error", kv.Value, err)
			continue
		}
		jobManager.AddJob(&job)
	}

	return nil
}

// AddJob 新加 Job
func (jobManager *JobManager) AddJob(jobConf *logic.AgentJobConfig) {
	jobManager.Lock()
	defer jobManager.Unlock()

	// 异常处理
	if jobConf.DispatchID == "" || jobConf.Name == "" {
		util.Log.Error("JobManager", "invalid jobConf", jobConf.Name, jobConf.DispatchID)
		return
	}

	// 判断重复加的情况，当原来的 Job 存在时，停止掉
	if job, ok := jobManager.jobs[jobConf.DispatchID]; ok {
		delete(jobManager.jobs, jobConf.DispatchID)
		job.Stop()
	}

	// 新建 Job
	job, err := NewJob(jobManager.ctx, jobConf, jobManager)
	if err != nil {
		util.Log.Error("JobManager", "job add failed", job.Name, job.DispatchID, err)
		return
	}

	jobManager.jobs[job.DispatchID] = job

	// 每个 Job 独立 go 程运行
	go func() {
		if err := job.ScheduleRun(); err != nil {
			util.Log.Error("JobManager", "job run failed", job.Name, job.DispatchID, err)
		}
	}()

	util.Log.Trace("JobManager", "add job", job.DispatchID, jobConf)
}

// UpdateJob 更新 Job
func (jobManager *JobManager) UpdateJob(jobConf *logic.AgentJobConfig) {
	// 先删后加
	jobManager.DelJob(jobConf)
	jobManager.AddJob(jobConf)
}

// DelJob 删除 Job
func (jobManager *JobManager) DelJob(jobConf *logic.AgentJobConfig) {
	jobManager.Lock()
	defer jobManager.Unlock()

	if job, ok := jobManager.jobs[jobConf.DispatchID]; ok {
		// 删除 Job
		delete(jobManager.jobs, jobConf.DispatchID)

		// 停止 Job
		job.Stop()

		util.Log.Trace("JobManager", "delete job", jobConf.DispatchID, jobConf)
	}
}

// Serve 用于发现 Jobs 修改
func (jobManager *JobManager) Serve() error {
	// 由于人为操作相对较小，ReWatch 不再重载配置
	if err := jobManager.load(); err != nil {
		return err
	}

ReWatch:
	key := fmt.Sprintf(logic.JobDispatchAgentPrefix, config.NodeID)
	jobCh := config.Storage.Watch(jobManager.ctx, key, v3.WithPrefix(), v3.WithPrevKV())

	for {
		select {

		case <-jobManager.ctx.Done():
			return nil

		case resp := <-jobCh:
			if resp.Err() != nil {
				util.Log.Error("JobManager", "subscribe", "watch failed", resp.Err().Error())
				time.Sleep(200 * time.Millisecond)
				goto ReWatch
			}

			for _, evt := range resp.Events {
				job := logic.AgentJobConfig{}

				switch evt.Type {
				case pb.PUT:
					err := job.Parse(evt.Kv.Value)
					if err != nil {
						util.Log.Error("JobManager", "subscribe", "job.Parse error", evt.Kv.Value, err)
						continue
					}
					jobManager.UpdateJob(&job)

				case pb.DELETE:
					err := job.Parse(evt.PrevKv.Value)
					if err != nil {
						util.Log.Error("JobManager", "subscribe", "job.Parse error", evt.PrevKv.Value, err)
						continue
					}
					jobManager.DelJob(&job)
				}
			}
		}
	}
}

// jobStop Job 停止后的回调
func (jobManager *JobManager) jobStop(dispatchID string) {
	// 删除执行结果记录
	key := fmt.Sprintf(logic.AgentExecutionRecordPrefix, dispatchID, config.NodeID)
	config.Storage.Delete(context.Background(), key, v3.WithPrefix())

	util.Log.Trace("JobManager", "delete job clean up resource", dispatchID, key)
}

// jobDone Job 执行完成后的回调
func (jobManager *JobManager) jobDone(jobConfig *logic.AgentJobConfig, result *logic.AgentExecutionResult) {
	// 判断是否需要记录日志、次数以及上报执行结果

	// 是否为无限执行任务
	isInfinite := jobConfig.Type == logic.JobTypeTimes && jobConfig.Times <= 0

	// 执行结果上报通知，用于 Dispatch 判断是否有父子任务
	// 注意：无限循环任务不支持父子任务，所以不需要记录
	if !isInfinite {
		// 复制一个新的 result，将 Output 置空
		ret := *result
		ret.Output = ""

		// 序列化数据
		v, err := json.Marshal(ret)
		if err != nil {
			util.Log.Error("agent", "jobDone", "json.Marshal error", ret, err)
			goto Next
		}

		// 最长执行结果通知保存时间为：任务执行超时时间 + 12h
		ttl := jobConfig.ExecutorTimeout + (12 * time.Hour)
		ttlLease, err := config.Storage.Grant(context.TODO(), int64(ttl.Seconds()))
		if err != nil {
			util.Log.Error("agent", "jobDone", "set status: get lease failed", jobConfig.Name, jobConfig.DispatchID, jobConfig.ExecutorTimeout, err)
			goto Next
		}

		// 写入执行结果
		key := fmt.Sprintf(logic.AgentExecutionRecord, ret.DispatchID, ret.AgentNode, ret.GroupID)
		_, err = config.Storage.Put(jobManager.ctx, key, string(v), v3.WithLease(ttlLease.ID))
		if err != nil {
			util.Log.Error("agent", "jobDone", "set status: put failed", jobConfig.Name, jobConfig.DispatchID, err)
			goto Next
		}
	}

Next:
	// 执行次数统计
	if jobManager.jobResultTimesStorage != nil {
		jobManager.jobResultTimesStorage.Put(result)
	}

	// 执行结果日志记录
	// 当无限循环任务 且 任务间隔小于1分钟 且 任务成功时，不记录执行结果日志
	log := true
	if isInfinite && jobConfig.TimesDelay < 1*time.Minute && result.Status == 0 {
		log = false
	}

	if log && jobManager.jobResultStorage != nil {
		// 内部 channel，不阻塞
		jobManager.jobResultStorage.Put(result)
	}
}
