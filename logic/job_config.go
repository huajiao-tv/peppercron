package logic

import (
	"encoding/json"
	"time"
)

type (
	// JobType 定义 Job 类型
	JobType int

	// ExecutorType 定义 Executor 类型
	ExecutorType int
)

const (
	// JobTypeSchedule 表示 JobType - 按计划执行
	JobTypeSchedule JobType = iota

	// JobTypeTimes 表示 JobType - 执行多少次
	JobTypeTimes
)

const (
	// ExecutorTypeShell 表示 ExecutorType - Shell 执行器
	ExecutorTypeShell ExecutorType = iota
	// ExecutorTypeGRPC 表示 ExecutorType - GPRC 请求执行器
	ExecutorTypeGRPC
	// ExecutorTypeHTTP 表示 ExecutorType - HTTP 请求执行器
	ExecutorTypeHTTP
)

// JobCallbackFunc Job 执行结果回调函数
type JobCallbackFunc func(*AgentExecutionResult)

// AgentJobConfig 定义 Agent 执行 Job 的结构
type AgentJobConfig struct {
	// Job 名
	Name string `json:"name"`

	// DispatchID
	DispatchID string `json:"dispatch_id"`

	// Job 类型，多少次或者间隔执行
	Type JobType `json:"type"`

	// Times 当 JobType 为 JobTypeTimes 时生效，执行次数配置
	Times int64 `json:"times"`

	// TimesDelay 为当 Times 大于
	TimesDelay time.Duration `json:"times_delay"`

	// Schedule 当 JobType 为 JobTypeSchedule 时生效，执行计划配置
	Schedule string `json:"schedule"`

	// 基础 Group ID
	BaseGroupID int64 `json:"base_group_id"`

	// Timezone 时区设置，空为本地时间
	Timezone string `json:"timezone"`

	// ExecutorBlocking 是否阻塞执行
	// 当 True 时，上个计划执行的任务未完成时，本次计划将不执行
	// 注意：只会在当 Type 为 JobTypeSchedule 时生效
	ExecutorBlocking bool `json:"executor_blocking"`

	// ExecutorType 执行器类型
	ExecutorType ExecutorType `json:"executor_type"`

	// ExecutorParams 执行器参数
	ExecutorParams []string `json:"executor_params"`

	// ExecutorTimeout 执行器超时
	ExecutorTimeout time.Duration `json:"executor_timeout"`

	// Extra environment variable to give to the command to execute.
	EnvironmentVariables []string `json:"environment_variables"`

	// Extras
	Extras map[string]string `json:"extras"`
}

// ToString 转换成 String
func (conf *AgentJobConfig) ToString() (string, error) {
	v, err := json.Marshal(conf)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// Parse 将 json 转换成 Job 对象
func (conf *AgentJobConfig) Parse(data []byte) error {
	return json.Unmarshal(data, conf)
}

// JobConfig describes a scheduled Job.
type JobConfig struct {
	AgentJobConfig

	// Job tag
	Tag string `json:"tag"`

	// Number of times to retry a job that failed an execution.
	Retries uint `json:"retries"`

	// Jobs that are dependent upon this one will be run after this job runs.
	DependentJobs []string `json:"dependent_jobs"`

	// Concurrency policy for this job
	Concurrency int `json:"concurrency"`

	// Computed job status
	Status string `json:"status"`
}

// ToString 转换成 String
func (conf *JobConfig) ToString() (string, error) {
	v, err := json.Marshal(conf)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

// Parse 将 json 转换成 Job 对象
func (conf *JobConfig) Parse(data []byte) error {
	return json.Unmarshal(data, conf)
}
