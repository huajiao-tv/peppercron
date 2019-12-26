package logic

import "time"

const (
	StatusOK = iota
	StatusFailed
)

type ExecutionResult struct {
	// Job name.
	JobName string `json:"job_name"`

	// DispatchID
	DispatchID string `json:"dispatch_id"`

	// Execution group id.
	GroupID int64 `json:"group_id"`

	// 按次数执行的任务，当次数完全执行完毕时，为 true
	JobCompleted bool `json:"job_completed"`
}

type AgentExecutionResult struct {
	ExecutionResult

	// Agent node id.
	AgentNode string `json:"agent_node"`

	// Start time of the execution.
	StartedAt time.Time `json:"started_at"`

	// When the execution finished running.
	FinishedAt time.Time `json:"finished_at"`

	// Execution status.
	Status int `json:"status"`

	// Partial output of the execution.
	Output string `json:"output"`
}
