//go:build unix

package audio

import "syscall"

var syscallEPIPE error = syscall.EPIPE
