//go:build linux || bsd || darwin

package asynq

func kill() error {
	return syscall.TerminateProcess(syscall.Getpid(), syscall.SIGTERM)
}
