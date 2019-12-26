package agent

import (
	"context"
	"time"
)

var (
	// ExecutorDefaultTimeout 执行器默认超时
	ExecutorDefaultTimeout = 1 * time.Hour
)

// Executor 执行器接口
// Run(参数) (执行结果，执行是否成功)
// Stop()
type Executor interface {
	// 运行
	Run(context.Context, time.Duration, []string) (string, bool)
	// 停止
	Stop()
}
