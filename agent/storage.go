package agent

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	// PG 数据库支持
	_ "github.com/lib/pq"

	"github.com/go-redis/redis"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
)

// JobResultStorage 存储 Job 结果的 Storage 接口
type JobResultStorage interface {
	Put(*logic.AgentExecutionResult)
}

const (
	// JobExecutionTimesKey 定义存储 Job 执行次数的 Key
	JobExecutionTimesKey = "job:times:%s"

	// JobExecutionTimesDateKey 定义存储 Job 执行次数的 Key
	JobExecutionTimesDateKey = "job:times:%s:%s"

	// JobExecutionTimesNodeKey 定义存储包含 Node 的 Job 执行次数的 Key
	JobExecutionTimesNodeKey = "job:times:node:%s:%s"

	// JobExecutionTimesNodeDateKey 定义存储包含 Node 的 Job 执行次数的 Key
	JobExecutionTimesNodeDateKey = "job:times:node:%s:%s:%s"

	// JobExecutionTimesSuccessField Job 执行次数成功字段
	JobExecutionTimesSuccessField = "success"
	// JobExecutionTimesFailedField Job 执行次数失败字段
	JobExecutionTimesFailedField = "failed"
)

// JobResultTimesRedisStorage 使用 Redis 存储 Job 执行次数的对象
type JobResultTimesRedisStorage struct {
	redis *redis.Client
}

// NewJobResultTimesRedisStorage 新建一个存储 Job 执行次数的 Redis 存储
func NewJobResultTimesRedisStorage(address, auth string, maxConnNum int, idleTimeout time.Duration) (*JobResultTimesRedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:               address,
		Password:           auth,
		DB:                 0,
		PoolSize:           maxConnNum,
		DialTimeout:        3 * time.Second,
		ReadTimeout:        3 * time.Second,
		WriteTimeout:       3 * time.Second,
		IdleTimeout:        3 * time.Second,
		IdleCheckFrequency: 1 * time.Second,
		MaxRetries:         2,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	storage := &JobResultTimesRedisStorage{
		redis: client,
	}

	return storage, nil
}

// Put 存储数据到存储
func (s *JobResultTimesRedisStorage) Put(data *logic.AgentExecutionResult) {
	// 执行次数统计
	timesKey := fmt.Sprintf(JobExecutionTimesKey, data.JobName)
	// hset 字段判断
	var field string
	if data.Status == logic.StatusOK {
		field = JobExecutionTimesSuccessField
	} else {
		field = JobExecutionTimesFailedField
	}
	// 总次数，无过期
	err := s.redis.HIncrBy(timesKey, field, 1).Err()
	if err != nil {
		util.Log.Error("JobResultTimesRedisStorage", "Incr times failed, redis error", err)
	}

	// 天次数，24小时过期
	day := time.Now().Format("20060102")
	timesDateKey := fmt.Sprintf(JobExecutionTimesDateKey, day, data.JobName)
	err = s.redis.HIncrBy(timesDateKey, field, 1).Err()
	if err != nil {
		util.Log.Error("JobResultTimesRedisStorage", "Incr times failed, redis error", err)
	}
	s.redis.Expire(timesDateKey, 24*time.Hour)

	// 节点次数，无过期
	timesNodeKey := fmt.Sprintf(JobExecutionTimesNodeKey, data.JobName, data.AgentNode)
	err = s.redis.HIncrBy(timesNodeKey, field, 1).Err()
	if err != nil {
		util.Log.Error("JobResultTimesRedisStorage", "Incr node times failed, redis error", err)
	}

	// 节点天次数，24小时过期
	timesNodeDateKey := fmt.Sprintf(JobExecutionTimesNodeDateKey, day, data.JobName, data.AgentNode)
	err = s.redis.HIncrBy(timesNodeDateKey, field, 1).Err()
	if err != nil {
		util.Log.Error("JobResultTimesRedisStorage", "Incr node times failed, redis error", err)
	}
	s.redis.Expire(timesNodeDateKey, 24*time.Hour)
}

// JobResultPGStorage 使用 PG 存储 Job 结果的对象
type JobResultPGStorage struct {
	db         *sql.DB
	dataCh     chan *logic.AgentExecutionResult
	datas      []*logic.AgentExecutionResult
	datasMutex sync.Mutex
}

// NewJobResultPGStorage Job 执行日志 PG 存储
func NewJobResultPGStorage(ctx context.Context, host, user, pass, dbname string) (*JobResultPGStorage, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, pass, host, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	// TODO: config
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(50)

	s := &JobResultPGStorage{
		db:         db,
		dataCh:     make(chan *logic.AgentExecutionResult, 10000),
		datas:      make([]*logic.AgentExecutionResult, 0, 1000),
		datasMutex: sync.Mutex{},
	}
	go s.Serve(ctx)

	return s, nil
}

// Put 将日志丢到队列
func (s *JobResultPGStorage) Put(data *logic.AgentExecutionResult) {
	select {
	case s.dataCh <- data:
	default:
		util.Log.Error("JobResultPGStorage", "Put data failed", "channel full")
	}
}

func (s *JobResultPGStorage) batchInsert(datas []*logic.AgentExecutionResult) {
	sqlStr := `INSERT INTO job_result(job_name, dispatch_id, group_id, job_completed, agent_node, started_at, finished_at, status, output_data) VALUES`
	vals := []interface{}{}

	for _, data := range datas {
		i := len(vals)
		sqlStr += fmt.Sprintf("($%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v, $%v),", i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9)
		vals = append(vals, data.JobName, data.DispatchID, data.GroupID, data.JobCompleted, data.AgentNode, data.StartedAt.UTC(), data.FinishedAt.UTC(), data.Status, data.Output)
	}

	sqlStr = strings.TrimSuffix(sqlStr, ",")

	_, err := s.db.Exec(sqlStr, vals...)
	if err != nil {
		util.Log.Error("JobResultPGStorage", "Batch insert data failed", sqlStr, err)
	}
}

// Serve 启动定期同步数据库服务
func (s *JobResultPGStorage) Serve(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.datasMutex.Lock()
			datas := s.datas
			s.datas = make([]*logic.AgentExecutionResult, 0, 1000)
			s.datasMutex.Unlock()

			s.batchInsert(datas)
			return
		case data := <-s.dataCh:
			var datas []*logic.AgentExecutionResult
			s.datasMutex.Lock()
			s.datas = append(s.datas, data)
			if len(s.datas) >= 1000 {
				datas = s.datas
				s.datas = make([]*logic.AgentExecutionResult, 0, 1000)
			}
			s.datasMutex.Unlock()

			if len(datas) > 0 {
				s.batchInsert(datas)
			}
		case <-ticker.C:
			var datas []*logic.AgentExecutionResult
			s.datasMutex.Lock()
			if len(s.datas) > 0 {
				datas = s.datas
				s.datas = make([]*logic.AgentExecutionResult, 0, 1000)
			}
			s.datasMutex.Unlock()

			if len(datas) > 0 {
				s.batchInsert(datas)
			}
		}
	}
}
