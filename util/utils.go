package util

import (
	"crypto/md5"
	"fmt"
)

// MD5Encrypt MD5 加密
func MD5Encrypt(data string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(data)))
}
