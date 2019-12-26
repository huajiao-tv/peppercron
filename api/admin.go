package api

import (
	"net"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/huajiao-tv/peppercron/config"
	"github.com/huajiao-tv/peppercron/dispatch"
)

type AdminServer struct {
}

func (s AdminServer) Serve(l net.Listener) error {
	router := gin.New()
	router.Any("/*action", s.HandleRequest)
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

func (s AdminServer) HandleRequest(c *gin.Context) {
	// parse path
	path := strings.Trim(c.Request.URL.Path, "/")
	urlParts := strings.Split(path, "/")
	parts := make([]string, 0, len(urlParts))
	for _, v := range urlParts {
		parts = append(parts, strings.Title(v))
	}
	// get method
	controller := reflect.ValueOf(&s)
	handler := strings.Join(parts, "") + "Handler"
	method := controller.MethodByName(handler)
	if !method.IsValid() {
		goto NotFound
	}
	// call
	method.Call([]reflect.Value{reflect.ValueOf(c)})
	return

NotFound:
	c.AbortWithStatus(http.StatusNotFound)
}

func (s AdminServer) LeaderHandler(c *gin.Context) {
	resp := Response{
		Data: dispatch.Leader(),
	}
	c.JSON(http.StatusOK, &resp)
}

func (s AdminServer) GetAgentsHandler(c *gin.Context) {
	if dispatch.Leader() != config.NodeID {
		c.JSON(http.StatusOK, "not leader")
		return
	}
	tag := c.GetString("tag")
	if tag == "" {
		c.JSON(http.StatusBadRequest, "no tag")
		return
	}
	c.JSON(http.StatusOK, dispatch.AgentManager.Get(tag))
}
