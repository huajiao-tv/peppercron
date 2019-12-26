package util

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/huajiao-tv/peppercron/config"
)

var (
	// Log 全局 Log 对象
	Log *Logger

	rootPath = func() string {
		ex, err := os.Executable()
		if err != nil {
			panic(err)
		}
		exPath := filepath.Dir(ex)
		return exPath
	}()
)

// InitLog 初始化全局 Log
func InitLog() error {
	backupDir := filepath.Join(rootPath, "log/backup/")
	os.MkdirAll(backupDir, os.ModePerm)
	filename := filepath.Join(rootPath, "log/", fmt.Sprintf("%s-%s", "peppercron", config.NodeID))
	log, err := NewLogger(filename, "peppercron", backupDir)
	if err != nil {
		return err
	}
	Log = log
	return nil
}
