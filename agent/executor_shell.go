package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"
)

// ExecutorShell Shell 执行器的具体实现
type ExecutorShell struct {
	env []string
}

func (executor *ExecutorShell) setEnv(env []string) {
	executor.env = env
}

// Run 具体的执行实现
func (executor *ExecutorShell) Run(pctx context.Context, timeout time.Duration, params []string) (string, bool) {
	if len(params) == 0 {
		// 根本就没参数，运行个鬼
		return "", false
	}

	if timeout == 0 {
		timeout = ExecutorDefaultTimeout
	}

	ctx, cancel := context.WithTimeout(pctx, timeout)
	defer cancel()

	// 构造 cmd
	cmd := exec.CommandContext(ctx, params[0], params[1:]...)

	// 设置环境变量
	cmd.Env = append(os.Environ(), executor.env...)

	// 执行并获取结果
	result, err := cmd.CombinedOutput()
	if err != nil {
		ps := cmd.ProcessState
		exitCode := ps.ExitCode()
		pid := ps.Pid()
		// usage := ps.SysUsage()

		return fmt.Sprintf("%v\n\npid: %v\nerror: %v\nexitCode: %v\n\n%v", string(result), pid, err, exitCode, ps.String()), false
	}

	return string(result), true
}

// Stop 停止执行器
func (executor *ExecutorShell) Stop() {
	// TODO：暂时不适用
	return
}
