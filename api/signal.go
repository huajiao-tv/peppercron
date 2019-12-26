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
	signal.Notify(ch, syscall.SIGINT, syscall.SIGUSR2)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			signal.Stop(ch)
			util.Log.Error("receive signal TERM/INT stop the process")
			for _, lis := range llist {
				lis.Close()
			}
			util.CancelFunc()
			<-time.After(GraceStopTimeout)
			util.Log.Error("signal handler done now end the main process")
			return
		case syscall.SIGUSR2:
			util.Log.Error("receive signal USR2 restart process")
			count, err := util.GraceNet.StartProcess()
			if err != nil {
				util.Log.Error("restart process fail", err)
				return
			}
			for _, lis := range llist {
				lis.Close()
			}
			util.CancelFunc()

			util.Log.Error("signal handler done now end the main process", count, err)
		}
	}
}
