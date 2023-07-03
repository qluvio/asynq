//go:build windows

package asynq

import "syscall"

func kill() error {
	handle, err := syscall.GetCurrentProcess()
	if err != nil {
		return err
	}
	return syscall.TerminateProcess(handle, 0)
}
