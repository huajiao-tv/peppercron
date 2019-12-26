package agent

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
)

// Job 定义执行 Job 的结构
type Job struct {
	*logic.AgentJobConfig

	// executor 具体的执行器
	executor Executor

	// schedule 具体的计划器
	schedule Schedule

	callback jobCallback

	ctx    context.Context
	cancel context.CancelFunc
}

// NewJob 新建一个 Job 实例
func NewJob(pctx context.Context, conf *logic.AgentJobConfig, callback jobCallback) (*Job, error) {
	// jobConf
	job := &Job{
		AgentJobConfig: conf,
		callback:       callback,
	}
	job.ctx, job.cancel = context.WithCancel(pctx)

	// 执行器
	switch job.ExecutorType {
	case logic.ExecutorTypeShell:
		executor := &ExecutorShell{}
		executor.setEnv(conf.EnvironmentVariables)
		job.executor = executor
	}

	return job, nil
}

// ScheduleRun 根据计划执行 Job
func (job *Job) ScheduleRun() error {
	switch job.Type {

	case logic.JobTypeTimes:
		// 按次数执行
		// 次数可以为无限，当次数配置小于等于0时作为无限循环任务
		i := int64(0)
		for {
			if job.Times > 0 && i == job.Times {
				return nil
			}
			if job.Times > 0 {
				i++
			}

			select {
			case <-job.ctx.Done():
				return nil
			default:
				util.Log.Debug("ScheduleRun", job.DispatchID, "Job Run")

				start := time.Now()
				result, ok := job.executor.Run(job.ctx, job.ExecutorTimeout, job.ExecutorParams)
				end := time.Now()
				usingTime := end.Sub(start)

				util.Log.Debug("ScheduleRun", job.DispatchID, "Job Done", usingTime.String(), ok, i)

				job.done(job.BaseGroupID+i, ok, result, start, end, i)

				// 执行延迟
				if job.TimesDelay != 0 {
					time.Sleep(job.TimesDelay)
				}
			}
		}

	case logic.JobTypeSchedule:
		// 按计划执行
		var err error
		job.schedule, err = defaultParser.Parse(job.Schedule)
		if err != nil {
			return err
		}

		// 时区处理
		var tz *time.Location
		if job.Timezone != "" {
			tz, _ = time.LoadLocation(job.Timezone)
		} else {
			tz = time.Local
		}

		var execNumber int64
		for {
			now := time.Now().In(tz)
			next := job.schedule.Next(now)
			groupID := next.UnixNano()

			select {
			case <-job.ctx.Done():
				return nil
			case <-time.After(next.Sub(now) + time.Second):
				execFunc := func() {
					atomic.AddInt64(&execNumber, 1)

					util.Log.Debug("ScheduleRun", job.DispatchID, "Job Run")

					start := time.Now()
					result, ok := job.executor.Run(job.ctx, job.ExecutorTimeout, job.ExecutorParams)
					end := time.Now()
					usingTime := end.Sub(start)

					util.Log.Debug("ScheduleRun", job.DispatchID, "Job Done", usingTime.String(), ok)

					job.done(groupID, ok, result, start, end, 0)
					atomic.AddInt64(&execNumber, -1)
				}

				// 阻塞执行
				// 当阻塞执行时，假设一个任务每一分钟执行一次，但是该任务有一次执行超过了1分钟，当该任务还在执行过程时，这一分钟将不触发新任务
				if job.ExecutorBlocking {
					execFunc()
				} else {
					// 不阻塞执行时，到点将会再次执行一个新任务，注意，这样会出现并发执行状态
					// 限制最大并发执行个数为10个
					// TODO: 10 改为配置化
					if atomic.LoadInt64(&execNumber) > 10 {
						util.Log.Error("ScheduleRun", job.DispatchID, "job no blocking execute number > 10")
						continue
					}

					go execFunc()
				}
			}
		}
	}

	return nil
}

// done Job 执行完成后，将调用该函数，返回结果等操作可放在该方法
// execTimes 执行次数，仅用于按次数执行用于判断任务是否完全完成
func (job *Job) done(groupID int64, ok bool, result string, start, end time.Time, execTimes int64) {
	status := 1
	if ok {
		status = 0
	}

	data := &logic.AgentExecutionResult{
		ExecutionResult: logic.ExecutionResult{
			JobName:    job.Name,
			DispatchID: job.DispatchID,
			GroupID:    groupID,
		},
		AgentNode:  config.NodeID,
		Status:     status,
		Output:     result,
		StartedAt:  start,
		FinishedAt: end,
	}

	// 判断任务是否完全完成
	// 只有在按次数执行时才有效，无限循环任务无效
	if job.Type == logic.JobTypeTimes && job.Times > 0 && job.Times == execTimes {
		data.JobCompleted = true
	}

	job.callback.jobDone(job.AgentJobConfig, data)
}

// Stop 停止 Job 计划执行
func (job *Job) Stop() {
	select {
	case <-job.ctx.Done():
	default:
		job.cancel()
	}
	job.executor.Stop()
	job.callback.jobStop(job.DispatchID)
}
