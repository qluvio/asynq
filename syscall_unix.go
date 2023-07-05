//go:build linux || bsd || darwin

package asynq

import "syscall"

func kill() error {
	return syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
}
