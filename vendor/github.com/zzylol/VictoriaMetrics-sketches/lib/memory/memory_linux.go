package memory

import (
	"syscall"

	"github.com/zzylol/VictoriaMetrics-sketches/lib/cgroup"
	"github.com/zzylol/VictoriaMetrics-sketches/lib/logger"
)

const maxInt = int(^uint(0) >> 1)

func sysTotalMemory() int {
	var si syscall.Sysinfo_t
	if err := syscall.Sysinfo(&si); err != nil {
		logger.Panicf("FATAL: error in syscall.Sysinfo: %s", err)
	}
	totalMem := maxInt
	if uint64(maxInt)/uint64(si.Totalram) > uint64(si.Unit) {
		totalMem = int(uint64(si.Totalram) * uint64(si.Unit))
	}
	mem := cgroup.GetMemoryLimit()
	if mem <= 0 || int64(int(mem)) != mem || int(mem) > totalMem {
		// Try reading hierarchical memory limit.
		// See https://github.com/zzylol/VictoriaMetrics-sketches/issues/699
		mem = cgroup.GetHierarchicalMemoryLimit()
		if mem <= 0 || int64(int(mem)) != mem || int(mem) > totalMem {
			return totalMem
		}
	}
	return int(mem)
}
