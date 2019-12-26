package api

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/huajiao-tv/peppercron/backend"
	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	v3 "go.etcd.io/etcd/clientv3"
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

func TestAddJobConfig(t *testing.T) {
	job := logic.JobConfig{
		AgentJobConfig: logic.AgentJobConfig{
			Name:         "echo_job",
			Type:         logic.JobTypeTimes,
			Times:        1,
			ExecutorType: logic.ExecutorTypeShell,
			ExecutorParams: []string{
				"echo",
				"hello",
			},
			ExecutorTimeout: time.Second,
		},
		Tag:           "test",
		Concurrency:   1,
		DependentJobs: []string{},
	}

	key := fmt.Sprintf(logic.JobConfiguration, job.Name)
	v, _ := json.Marshal(job)
	config.Storage.Put(context.TODO(), key, string(v))
}
