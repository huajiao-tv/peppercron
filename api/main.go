package api

import (
	"os"
	"sync"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/util"
)

const (
	HttpReadTimeout  = 10 * time.Second
	HttpWriteTimeout = 10 * time.Second
)

func Serve() {
	l1, err := util.GraceNet.Listen("tcp", config.RemoteConf().FrontPort)
	if err != nil {
		panic(err)
	}
	l2, err := util.GraceNet.Listen("tcp", config.RemoteConf().AdminPort)
	if err != nil {
		panic(err)
	}

	go signalHandler(l1, l2)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		err = ApiServer{}.Serve(l1)
		util.Log.Error("front serve finish err", err)
	}()

	go func() {
		defer wg.Done()
		err := AdminServer{}.Serve(l2)
		util.Log.Error("admin serve finish err", err)
	}()

	wg.Wait()
	util.Log.Error("service stopped", err)
	util.Log.Error("cron process end", os.Getpid())
}
