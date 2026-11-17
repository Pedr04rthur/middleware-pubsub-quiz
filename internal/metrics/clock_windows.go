package metrics

import (
	"syscall"
	"time"
	"unsafe"
)

var (
	kernel32        = syscall.NewLazyDLL("kernel32.dll")
	procQPCounter   = kernel32.NewProc("QueryPerformanceCounter")
	procQPFrequency = kernel32.NewProc("QueryPerformanceFrequency")
	qpcFrequency    = queryPerformanceFrequency()
	fallbackStart   = time.Now()
)

func queryPerformanceFrequency() int64 {
	var f int64
	if r, _, _ := procQPFrequency.Call(uintptr(unsafe.Pointer(&f))); r == 0 {
		return 0
	}
	return f
}

func nanotime() int64 {
	if qpcFrequency <= 0 {
		return int64(time.Since(fallbackStart))
	}
	var c int64
	if r, _, _ := procQPCounter.Call(uintptr(unsafe.Pointer(&c))); r == 0 {
		return int64(time.Since(fallbackStart))
	}
	// Divide antes de multiplicar para não estourar int64.
	return c/qpcFrequency*int64(time.Second) + c%qpcFrequency*int64(time.Second)/qpcFrequency
}
