package dispatch

import (
	"context"
	"sync"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
	v3 "go.etcd.io/etcd/clientv3"
	pb "go.etcd.io/etcd/mvcc/mvccpb"
)

type AgentUpdate interface {
	AgentUp(node string, tags []string)
	AgentDown(node string, tags []string)
}

type JobDispatcherManager struct {
	sync.RWMutex
	tagJobs map[string]map[string]Dispatcher

	ctx context.Context
}

func NewJobDispatcherManager(pctx context.Context) *JobDispatcherManager {
	mgr := &JobDispatcherManager{
		ctx:     pctx,
		tagJobs: make(map[string]map[string]Dispatcher),
	}
	return mgr
}

func (mgr *JobDispatcherManager) Serve() {
	// recover running jobs
	// 由于人为操作相对较小，ReWatch 不再重载配置
	mgr.recover()

ReWatch:
	jobsCh := config.Storage.Watch(mgr.ctx, logic.JobConfigurationPrefix, v3.WithPrefix(), v3.WithPrevKV())

	for {
		select {

		case <-mgr.ctx.Done():
			return

		case resp := <-jobsCh:
			if resp.Err() != nil {
				util.Log.Error("JobDispatcherManager", "Serve", "watch failed", resp.Err().Error())
				time.Sleep(200 * time.Millisecond)
				goto ReWatch
			}

			// global update for job configurations
			for _, evt := range resp.Events {
				jobConfig := logic.JobConfig{}

				switch evt.Type {

				case pb.PUT:
					if err := jobConfig.Parse(evt.Kv.Value); err != nil {
						util.Log.Error("JobDispatcherManager", "Serve", "put job parse failed", err)
						continue
					}
					mgr.addJob(&jobConfig)

				case pb.DELETE:
					if err := jobConfig.Parse(evt.PrevKv.Value); err != nil {
						util.Log.Error("JobDispatcherManager", "Serve", "delete job parse failed", err)
						continue
					}
					mgr.removeJob(&jobConfig)
				}
			}
		}
	}
}

func (mgr *JobDispatcherManager) recover() {
	resp, err := config.Storage.Get(mgr.ctx, logic.JobConfigurationPrefix, v3.WithPrefix())
	if err != nil {
		// util.Log.Error("JobDispatcherManager", "recover", "get job config failed", err)
		return
	}
	for _, kv := range resp.Kvs {
		jobConfig := logic.JobConfig{}
		if err := jobConfig.Parse(kv.Value); err != nil {
			util.Log.Error("JobDispatcherManager", "recover", "job config parse failed", err)
			continue
		}
		mgr.addJob(&jobConfig)
	}
}

func (mgr *JobDispatcherManager) addJob(conf *logic.JobConfig) {
	// new job dispatcher
	dispatcher := NewJobDispatcher(mgr.ctx, conf)

	mgr.Lock()
	tm, ok := mgr.tagJobs[conf.Tag]
	if !ok {
		tm = make(map[string]Dispatcher)
		mgr.tagJobs[conf.Tag] = tm
	}
	old, stop := tm[conf.Name]
	tm[conf.Name] = dispatcher
	mgr.Unlock()

	if stop {
		old.Stop()
	}
	go dispatcher.Serve()
}

func (mgr *JobDispatcherManager) removeJob(conf *logic.JobConfig) {
	mgr.Lock()
	defer mgr.Unlock()

	//
	tm, ok := mgr.tagJobs[conf.Tag]
	if !ok {
		return
	}
	old, ok := tm[conf.Name]
	if !ok {
		return
	}
	delete(tm, conf.Name)
	old.Stop()
}

func (mgr *JobDispatcherManager) AgentUp(node string, tags []string) {
	// notify job dispatcher
	mgr.RLock()
	defer mgr.RUnlock()
	for _, tag := range tags {
		tm, ok := mgr.tagJobs[tag]
		if !ok {
			continue
		}
		util.Log.Debug("JobDispatcherManager", "Debug AgentUp !!!", "tm:", tm)
		for _, jd := range tm {
			jd.AgentUp(node)
		}
	}
}

func (mgr *JobDispatcherManager) AgentDown(node string, tags []string) {
	// notify job dispatcher
	mgr.RLock()
	defer mgr.RUnlock()
	for _, tag := range tags {
		tm, ok := mgr.tagJobs[tag]
		if !ok {
			continue
		}
		util.Log.Debug("JobDispatcherManager", "Debug AgentDown !!!", "tm:", tm)
		for _, jd := range tm {
			jd.AgentDown(node)
		}
	}
}
