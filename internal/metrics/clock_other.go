//go:build !windows

package metrics

import "time"

var clockStart = time.Now()

func nanotime() int64 {
	return int64(time.Since(clockStart))
}
