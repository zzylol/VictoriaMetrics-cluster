//go:build freebsd || openbsd || dragonfly || netbsd

package memory

import (
	"github.com/zzylol/VictoriaMetrics-sketches/lib/logger"
)

// This code has been adopted from https://github.com/pbnjay/memory

func sysTotalMemory() int {
	s, err := sysctlUint64("hw.physmem")
	if err != nil {
		logger.Panicf("FATAL: cannot determine system memory: %s", err)
	}
	return int(s)
}
