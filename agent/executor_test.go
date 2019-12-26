package agent

import (
	"context"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	executor := &ExecutorShell{}

	// 检查执行是否正常
	result, ok := executor.Run(context.Background(), 0, []string{"echo", "1", "2", "3"})
	if !ok {
		t.Error("失败：echo 都不能执行？")
	}

	// 检查结果
	if result != "1 2 3\n" {
		t.Error("失败：结果不对啊，", result)
	}
}

func TestRunWithTimeout(t *testing.T) {
	executor := &ExecutorShell{}

	_, ok := executor.Run(context.Background(), 1*time.Second, []string{"sleep", "10"})
	if ok {
		t.Error("失败：timeout 没生效")
	}
}

func TestRunWithEnv(t *testing.T) {
	executor := &ExecutorShell{}
	executor.setEnv([]string{"A=AA"})

	result, ok := executor.Run(context.Background(), 0, []string{"sh", "-c", "echo $A"})
	if !ok {
		t.Error("失败：echo 都不能执行？")
	}

	// 检查结果
	if result != "AA\n" {
		t.Error("失败：结果不对啊，", result)
	}
}
