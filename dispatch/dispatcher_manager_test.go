package dispatch

import (
	"context"
	"fmt"
	"testing"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
)

func TestJobDispatcherManager(t *testing.T) {
	// create instance
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	// start
	m := NewJobDispatcherManager(ctx)
	go m.Serve()

	job := &logic.JobConfig{
		AgentJobConfig: logic.AgentJobConfig{
			DispatchID: "TestJobDispatcherManager",
			Name:       "TestJobDispatcherManager",
			Type:       logic.JobTypeTimes,
		},
		Tag:           "",
		DependentJobs: []string{},
		Concurrency:   2,
	}
	v, err := job.ToString()
	if err != nil {
		t.Fatal("marshal job failed", err)
	}
	key := fmt.Sprintf(logic.JobConfiguration, job.Name)
	_, err = config.Storage.Put(ctx, key, string(v))
	if err != nil {
		t.Fatal("put job failed", err)
	}
	// TODO
}
