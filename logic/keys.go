package logic

const (
	// KeyPrefix 前缀
	KeyPrefix = "/pepper/cron"

	// DispatchElectionMaster 用于主从选举
	DispatchElectionMaster = KeyPrefix + "/election/master"
	// JobConfigurationPrefix Job 配置，Agent 不用这里的
	JobConfigurationPrefix = KeyPrefix + "/jobs"
	// JobConfiguration Job 具体的配置 Key
	JobConfiguration = JobConfigurationPrefix + "/%v"
	// JobDispatchRecord Job 分配的状态记录
	JobDispatchRecord = KeyPrefix + "/dispatch/%v"
	// BaseJobExecutionRecordPrefix Job 执行结果记录
	BaseJobExecutionRecordPrefix = KeyPrefix + "/job/execution"
	// JobExecutionRecordPrefix Job 执行结果记录
	JobExecutionRecordPrefix = BaseJobExecutionRecordPrefix + "/%v"
	// JobExecutionRecord Job 执行结果记录
	JobExecutionRecord = JobExecutionRecordPrefix + "/%v"

	// Write by Dispatcher and Read by Agent

	//JobDispatchAgentPrefix 分配器下发给 Agent 的 Jobs
	JobDispatchAgentPrefix = KeyPrefix + "/agent/%v/jobs"
	// JobDispatchAgent 具体某个 Agent 的配置
	JobDispatchAgent = JobDispatchAgentPrefix + "/%v"

	// Write by Agent and Read by Dispatcher

	// AgentNodeAlivePrefix Agent KeepAlive 上报 Key
	AgentNodeAlivePrefix = KeyPrefix + "/agent/nodes/alive"
	// AgentNodeAlive 同上
	AgentNodeAlive = AgentNodeAlivePrefix + "/%v"
	// BaseAgentExecutionRecordPrefix Agent 执行结果上报 Key
	BaseAgentExecutionRecordPrefix = KeyPrefix + "/agent/execution/%v"
	// AgentExecutionRecordPrefix 同上
	AgentExecutionRecordPrefix = BaseAgentExecutionRecordPrefix + "/%v"
	// AgentExecutionRecord 同上
	AgentExecutionRecord = AgentExecutionRecordPrefix + "/%v" // dispatch_id + node_id + group_id

	// Read-Write by Agent

	// Read-Only by All
	GlobalConfig = KeyPrefix + "/conf/global"
	NodeConfig   = KeyPrefix + "/conf/nodes/%v"
)
