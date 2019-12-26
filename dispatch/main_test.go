package dispatch

import (
	"context"
	"flag"
	"strings"
	"sync"

	"github.com/huajiao-tv/peppercron/backend"
	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	v3 "go.etcd.io/etcd/clientv3"
)

const (
	TestAgent          = "127.0.0.1"
	TestAgent2         = "127.0.0.2"
	TestAgent3         = "127.0.0.3"
	TestTag            = "tag_test"
	TestConcurrencyTag = "tag_concurrency_test"
	TestNodeDownTag    = "tag_node_down_test"
)

func init() {
	var endPoints, user, password string
	flag.StringVar(&endPoints, "e", "", "etcd end points")
	flag.StringVar(&user, "u", "", "etcd user name")
	flag.StringVar(&password, "p", "", "etcd user password")
	flag.Parse()

	config.Storage, _ = backend.NewStorage(strings.Split(endPoints, ","), user, password)
	config.Storage.Delete(context.TODO(), logic.KeyPrefix, v3.WithPrefix())
}

type TestDispatcher struct {
	sync.RWMutex
	jobName     string
	concurrency int
	jobRes      *JobExecutionResult
	depRes      map[string]*JobExecutionResult
}

func (td *TestDispatcher) AgentUp(string) {
}

func (td *TestDispatcher) AgentDown(string) {
}

func (td *TestDispatcher) Name() string {
	return td.jobName
}

func (td *TestDispatcher) Concurrency() int {
	return td.concurrency
}

func (td *TestDispatcher) Serve() {
}

func (td *TestDispatcher) Stop() {
}

func (td *TestDispatcher) DependencyCompleted(res *JobExecutionResult) {
	td.Lock()
	defer td.Unlock()
	td.depRes[res.JobName] = res
}

func (td *TestDispatcher) AgentsCompleted(res *JobExecutionResult) {
	td.Lock()
	defer td.Unlock()
	td.jobRes = res
}

func (td *TestDispatcher) GetJobResult() *JobExecutionResult {
	td.RLock()
	defer td.RUnlock()
	return td.jobRes
}

func (td *TestDispatcher) GetDepResult(name string) *JobExecutionResult {
	td.RLock()
	defer td.RUnlock()
	return td.depRes[name]
}
