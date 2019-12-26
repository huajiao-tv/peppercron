package backend

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/huajiao-tv/peppercron/logic"
	v3 "go.etcd.io/etcd/clientv3"
)

const (
	KeyPrefix = "/pepper/cron/test/storage"
)

var (
	storage *Storage
)

func init() {
	rand.Seed(time.Now().UnixNano())

	var endPoints, user, password string
	flag.StringVar(&endPoints, "e", "", "etcd end points")
	flag.StringVar(&user, "u", "", "etcd user name")
	flag.StringVar(&password, "p", "", "etcd user password")
	flag.Parse()

	storage, _ = NewStorage(strings.Split(endPoints, ","), user, password)
	storage.Delete(context.TODO(), logic.KeyPrefix, v3.WithPrefix())
}

func TestStorage_Get(t *testing.T) {
	key := fmt.Sprintf(KeyPrefix+"/TestStorage_Get/%v", time.Now().UnixNano())
	_, err := storage.Get(context.TODO(), key)
	if err != nil {
		t.Fatal("get error", err)
	}
}

func TestStorage_Put(t *testing.T) {
	key := fmt.Sprintf(KeyPrefix+"/TestStorage_Put/%v", time.Now().UnixNano())
	_, err := storage.Put(context.TODO(), key, key)
	if err != nil {
		t.Fatal("put error", err)
	}
}

func TestStorage_PutGet(t *testing.T) {
	key := fmt.Sprintf(KeyPrefix+"/TestStorage_PutGet/%v", time.Now().UnixNano())
	_, err := storage.Put(context.TODO(), key, key)
	if err != nil {
		t.Fatal("put error", err)
	}
	resp, err := storage.Get(context.TODO(), key)
	if err != nil {
		t.Fatal("get error", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("get result error", err)
	}
	if string(resp.Kvs[0].Value) != key {
		t.Fatal("get result changed", err)
	}
}

func TestStorage_GetWithCancel(t *testing.T) {
	key := fmt.Sprintf(KeyPrefix+"/TestStorage_GetWithCancel/%v", time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := storage.Get(ctx, key)
	if err != context.Canceled {
		t.Fatal("cancel err", err)
	}
}
