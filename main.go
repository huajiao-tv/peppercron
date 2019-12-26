package main

import (
	"os"

	"github.com/huajiao-tv/peppercron/agent"
	"github.com/huajiao-tv/peppercron/api"
	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/dispatch"
	"github.com/huajiao-tv/peppercron/util"
)

func init() {
	util.InitContext()
	if err := util.InitLog(); err != nil {
		panic(err)
	}
	if err := config.Init(util.Context); err != nil {
		panic(err)
	}
}

func main() {
	println("start cron: ", os.Getpid())

	go agent.Run(util.Context)

	go dispatch.Run(util.Context)

	api.Serve()

	println("stop cron: ", os.Getpid())
	os.Exit(0)
}
