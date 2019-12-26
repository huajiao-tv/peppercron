package api

import (
	"fmt"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/logic"
	"github.com/huajiao-tv/peppercron/util"
)

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

type ApiServer struct {
}

func (s ApiServer) Serve(l net.Listener) error {
	router := gin.New()
	s.ApiRoutes(router)
	server := &http.Server{
		ReadTimeout:  HttpReadTimeout,
		WriteTimeout: HttpWriteTimeout,
		Handler:      router,
	}
	if err := server.Serve(l); err != nil {
		return err
	}
	return nil
}

func (s ApiServer) ApiRoutes(r *gin.Engine) {
	v1 := r.Group("/v1")
	v1.POST("/jobs", s.CreateJobHandler)
	//v1.PATCH("/jobs", s.jobCreateOrUpdateHandler)
	//// Place fallback routes last
	//v1.GET("/jobs", s.jobsHandler)

	jobs := v1.Group("/jobs")
	jobs.DELETE("/:job", s.DeleteJobHandler)
	//jobs.POST("/:job", s.jobRunHandler)
	//jobs.POST("/:job/toggle", s.jobToggleHandler)
	//
	//// Place fallback routes last
	//jobs.GET("/:job", s.jobGetHandler)
	//jobs.GET("/:job/executions", s.executionsHandler)
}

func (s ApiServer) CreateJobHandler(c *gin.Context) {
	var job logic.JobConfig
	if err := c.BindJSON(&job); err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	jobString, _ := job.ToString()
	util.Log.Trace("[API] create job request", job.Name, jobString)

	// save job on storage
	key := fmt.Sprintf(logic.JobConfiguration, job.Name)
	jobConf, err := job.ToString()
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	if _, err := config.Storage.Put(c.Request.Context(), key, jobConf); err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	c.JSON(http.StatusOK, &Response{})
}

func (s ApiServer) DeleteJobHandler(c *gin.Context) {
	jobName := c.Param("job")
	if jobName == "" {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	util.Log.Trace("[API] delete job request", jobName)

	// delete job on storage
	key := fmt.Sprintf(logic.JobConfiguration, jobName)
	if _, err := config.Storage.Delete(c.Request.Context(), key); err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	c.JSON(http.StatusOK, &Response{})
}
