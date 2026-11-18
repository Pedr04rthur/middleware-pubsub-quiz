package metrics

import (
	"syscall"
	"time"
)

// processCPUTime retorna o tempo de CPU (usuário + kernel) usado pelo processo.
func processCPUTime() (time.Duration, bool) {
	process, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, false
	}
	var creation, exit, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(process, &creation, &exit, &kernel, &user); err != nil {
		return 0, false
	}
	return filetimeDuration(kernel) + filetimeDuration(user), true
}

// Filetime de GetProcessTimes é uma duração em unidades de 100ns.
func filetimeDuration(ft syscall.Filetime) time.Duration {
	return time.Duration(int64(ft.HighDateTime)<<32|int64(ft.LowDateTime)) * 100
}
