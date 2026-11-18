//go:build unix

package metrics

import (
	"syscall"
	"time"
)

// processCPUTime retorna o tempo de CPU (usuário + sistema) usado pelo processo.
func processCPUTime() (time.Duration, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, false
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), true
}
