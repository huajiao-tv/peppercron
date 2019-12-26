package config

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/huajiao-tv/peppercron/logic"
	pb "go.etcd.io/etcd/mvcc/mvccpb"
)

const (
	DefaultTags      = "test"
	DefaultFrontPort = ":12306"
	DefaultAdminPort = ":12307"
)

// SettingGlobal 全局配置
type SettingGlobal struct {
	FrontPort string `json:"front_port"`
	AdminPort string `json:"admin_port"`

	JobResultTimesStorage *SettingJobResultStorage `json:"job_result_times_storage"`
	JobResultStorage      *SettingJobResultStorage `json:"job_result_storage"`
}

// SettingJobResultStorage Job 结果存储
type SettingJobResultStorage struct {
	Type        string        `json:"type"`
	Addr        string        `json:"addr"`
	Auth        string        `json:"auth"`
	MaxConnNum  int           `json:"max_conn_num"`
	IdleTimeout time.Duration `json:"idle_timeout"`
	User        string        `json:"user"`
	Database    string        `json:"database"`
}

// SettingNode 节点配置
type SettingNode struct {
	Tags string `json:"tags"`
}

// Setting 配置
type Setting struct {
	SettingGlobal
	SettingNode
}

func getRemoteConfig(ctx context.Context) error {
	setting := Setting{
		SettingGlobal: SettingGlobal{
			FrontPort: DefaultFrontPort,
			AdminPort: DefaultAdminPort,
			JobResultStorage: &SettingJobResultStorage{
				Type:     "pg",
				Addr:     "gptest01.bjyt.gpdb.soft.360.cn:5432",
				User:     "pepper_cron",
				Auth:     "0900b81f7ce06cfa",
				Database: "pepper_cron",
			},
			JobResultTimesStorage: &SettingJobResultStorage{
				Type:        "redis",
				Addr:        "10.142.97.30:1554",
				Auth:        "79b42b7404e2d3cc",
				MaxConnNum:  50,
				IdleTimeout: 3 * time.Second,
			},
		},
		SettingNode: SettingNode{
			Tags: DefaultTags,
		},
	}

	nodeKey := fmt.Sprintf(logic.NodeConfig, NodeID)

	// get global conf
	resp, err := Storage.Get(ctx, logic.GlobalConfig)
	if err != nil {
		return err
	}
	if len(resp.Kvs) > 0 {
		err = json.Unmarshal(resp.Kvs[0].Value, &setting.SettingGlobal)
		if err != nil {
			return err
		}
	}

	// get node conf
	resp, err = Storage.Get(ctx, nodeKey)
	if err != nil {
		return err
	}
	if len(resp.Kvs) > 0 {
		err = json.Unmarshal(resp.Kvs[0].Value, &setting.SettingNode)
		if err != nil {
			return err
		}
	}

	//
	UpdateConf(&setting)

	return nil
}

func GetRemoteConfig(ctx context.Context) error {
	if err := getRemoteConfig(ctx); err != nil {
		return err
	}

	// monitor etcd changes
	// go subscribeConfig(ctx, globalChan, nodeChan)
	go subscribeGlobalConfig(ctx)
	go subscribeNodeConfig(ctx)

	return nil
}

func subscribeNodeConfig(ctx context.Context) {
ReWatch:
	nodeKey := fmt.Sprintf(logic.NodeConfig, NodeID)
	nodeChan := Storage.Watch(ctx, nodeKey)

	if err := getRemoteConfig(ctx); err != nil {
		fmt.Println("subscribeConfig", "node config get failed", err)
		return
	}

	for {
		setting := &Setting{
			SettingGlobal: RemoteConf().SettingGlobal,
			SettingNode:   RemoteConf().SettingNode,
		}
		select {
		case <-ctx.Done():
			return
		case nodeResp := <-nodeChan:
			if nodeResp.Err() != nil {
				fmt.Println("subscribeConfig", "node config watch failed", nodeResp.Err())
				time.Sleep(200 * time.Millisecond)
				goto ReWatch
			}

			if len(nodeResp.Events) == 0 {
				continue
			}
			fmt.Println("subscribeConfig", "node config change", nodeResp.Events)
			evt := nodeResp.Events[len(nodeResp.Events)-1]
			if evt.Type != pb.PUT {
				fmt.Println("IGNORE: node config deleted...")
				continue
			}
			nodeConf := SettingNode{}
			if err := json.Unmarshal(evt.Kv.Value, &nodeConf); err != nil {
				fmt.Println("IGNORE: invalid node config", err.Error())
				continue
			}
			setting.SettingNode = nodeConf
		}
		UpdateConf(setting)
	}
}

func subscribeGlobalConfig(ctx context.Context) {
ReWatch:
	globalChan := Storage.Watch(ctx, logic.GlobalConfig)

	if err := getRemoteConfig(ctx); err != nil {
		fmt.Println("subscribeConfig", "global config get failed", err)
		return
	}

	for {
		setting := &Setting{
			SettingGlobal: RemoteConf().SettingGlobal,
			SettingNode:   RemoteConf().SettingNode,
		}
		select {
		case <-ctx.Done():
			return
		case globalResp := <-globalChan:
			if globalResp.Err() != nil {
				fmt.Println("subscribeConfig", "global config watch failed", globalResp.Err())
				time.Sleep(200 * time.Millisecond)
				goto ReWatch
			}
			if len(globalResp.Events) == 0 {
				continue
			}
			fmt.Println("subscribeConfig", "global config change", globalResp.Events)
			evt := globalResp.Events[len(globalResp.Events)-1]
			if evt.Type != pb.PUT {
				fmt.Println("IGNORE: global config deleted...")
				continue
			}
			globalConf := SettingGlobal{}
			if err := json.Unmarshal(evt.Kv.Value, &globalConf); err != nil {
				fmt.Println("IGNORE: invalid global config", err.Error())
				continue
			}
			setting.SettingGlobal = globalConf
		}
		UpdateConf(setting)
	}
}

// func subscribeConfig(ctx context.Context, globalChan, nodeChan clientv3.WatchChan) {
// 	for {
// 		setting := &Setting{
// 			SettingGlobal: RemoteConf().SettingGlobal,
// 			SettingNode:   RemoteConf().SettingNode,
// 		}
// 		select {
// 		case <-ctx.Done():
// 			return
//
// 		case globalResp := <-globalChan:
// 			if len(globalResp.Events) == 0 {
// 				continue
// 			}
// 			fmt.Println("subscribeConfig", "global config change", globalResp.Events)
// 			evt := globalResp.Events[len(globalResp.Events)-1]
// 			if evt.Type != pb.PUT {
// 				fmt.Println("IGNORE: global config deleted...")
// 				continue
// 			}
// 			globalConf := SettingGlobal{}
// 			if err := json.Unmarshal(evt.Kv.Value, &globalConf); err != nil {
// 				fmt.Println("IGNORE: invalid global config", err.Error())
// 				continue
// 			}
// 			setting.SettingGlobal = globalConf
//
// 		case nodeResp := <-nodeChan:
// 			if len(nodeResp.Events) == 0 {
// 				continue
// 			}
// 			fmt.Println("subscribeConfig", "node config change", nodeResp.Events)
// 			evt := nodeResp.Events[len(nodeResp.Events)-1]
// 			if evt.Type != pb.PUT {
// 				fmt.Println("IGNORE: node config deleted...")
// 				continue
// 			}
// 			nodeConf := SettingNode{}
// 			if err := json.Unmarshal(evt.Kv.Value, &nodeConf); err != nil {
// 				fmt.Println("IGNORE: invalid node config", err.Error())
// 				continue
// 			}
// 			setting.SettingNode = nodeConf
// 		}
//
// 		//
// 		UpdateConf(setting)
// 	}
// }
