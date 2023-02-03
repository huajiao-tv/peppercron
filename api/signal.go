package api

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/huajiao-tv/peppercron/util"
)

const (
	GraceStopTimeout = time.Second * 5
)

func signalHandler(llist ...net.Listener) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGUSR2) // bug: 没有注册 syscall.SIGTERM
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:

			// 令 ch 停止接收信号
			signal.Stop(ch)
			util.Log.Error("receive signal TERM/INT stop the process")

			// 关闭所有 listener
			for _, lis := range llist {
				lis.Close()
			}

			// 全局 context cancel
			util.CancelFunc()

			// 等待一段时间
			<-time.After(GraceStopTimeout)
			util.Log.Error("signal handler done now end the main process")
			return
		case syscall.SIGUSR2:

			util.Log.Error("receive signal USR2 restart process")

			// 启动一个新进程
			count, err := util.GraceNet.StartProcess()
			if err != nil {
				util.Log.Error("restart process fail", err)
				return
			}

			// 关闭原来的 listener
			for _, lis := range llist {
				lis.Close()
			}

			// 全局 context cancel
			util.CancelFunc()

			util.Log.Error("signal handler done now end the main process", count, err)
		}
	}
}
