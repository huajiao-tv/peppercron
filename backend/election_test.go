package backend

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/huajiao-tv/peppercron/logic"
	"go.etcd.io/etcd/clientv3/concurrency"
)

func TestElection(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())

	// election
	el1, err := storage.NewElection(logic.DispatchElectionMaster)
	if err != nil {
		t.Fatal("new election1 failed", err)
	}
	el2, err := storage.NewElection(logic.DispatchElectionMaster)
	if err != nil {
		t.Fatal("new election2 failed", err)
	}

	// create candidate
	electc := make(chan *concurrency.Election, 2)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		// sleep so that el2 wins
		time.Sleep(time.Second * 2)
		err := el1.Campaign(ctx, "el1")
		if err != nil && err != context.Canceled {
			t.Fatal("election1 campaign failed", err)
		}
		t.Log("election 1 existing...")
		electc <- el1
	}()
	go func() {
		defer wg.Done()
		err := el2.Campaign(ctx, "el2")
		if err != nil && err != context.Canceled {
			t.Fatal("election2 campaign failed", err)
		}
		t.Log("election 2 existing...")
		electc <- el2
	}()

	// check leader
	e := <-electc
	if string((<-e.Observe(ctx)).Kvs[0].Value) != "el2" {
		t.Fatal("election failed")
	}
	resp, err := el1.Leader(ctx)
	if err != nil {
		t.Fatal("el1 get leader failed", err)
	}
	if string(resp.Kvs[0].Value) != "el2" {
		t.Fatal("el1 get leader failed", err)
	}
	resp, err = el2.Leader(ctx)
	if err != nil {
		t.Fatal("el2 get leader failed", err)
	}
	if string(resp.Kvs[0].Value) != "el2" {
		t.Fatal("el2 get leader failed", err)
	}

	// reset leader
	if err := el2.Resign(ctx); err != nil {
		t.Fatal("resign failed", err)
	}
	// check leader
	e = <-electc
	if string((<-e.Observe(ctx)).Kvs[0].Value) != "el1" {
		t.Fatal("election failed")
	}
	resp, err = el1.Leader(ctx)
	if err != nil {
		t.Fatal("el1 get leader failed", err)
	}
	if string(resp.Kvs[0].Value) != "el1" {
		t.Fatal("el1 get leader failed", err)
	}
	resp, err = el2.Leader(ctx)
	if err != nil {
		t.Fatal("el2 get leader failed", err)
	}
	if string(resp.Kvs[0].Value) != "el1" {
		t.Fatal("el2 get leader failed", err)
	}

	// stop test
	stop()
	wg.Wait()
}
