package util

import "context"

var (
	// Context 全局 Context
	Context context.Context
	// CancelFunc 全局 Context 取消方法
	CancelFunc context.CancelFunc
)

// InitContext 初始化全局 Context
func InitContext() {
	Context, CancelFunc = func() (context.Context, context.CancelFunc) {
		return context.WithCancel(context.Background())
	}()
}
