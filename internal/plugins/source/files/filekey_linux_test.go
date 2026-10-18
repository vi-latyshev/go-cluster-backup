//go:build linux

package files

import "syscall"

// setChangeTime sets the inode change time of st, which Linux keeps in Ctim.
func setChangeTime(st *syscall.Stat_t, sec, nsec int64) {
	st.Ctim = syscall.Timespec{Sec: sec, Nsec: nsec}
}
