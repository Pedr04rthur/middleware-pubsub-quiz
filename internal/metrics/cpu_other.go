//go:build !unix && !windows

package metrics

import "time"

func processCPUTime() (time.Duration, bool) {
	return 0, false
}
