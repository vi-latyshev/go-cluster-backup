//go:build darwin

package files

import "syscall"

// setChangeTime sets the inode change time of st, which Darwin keeps in Ctimespec.
func setChangeTime(st *syscall.Stat_t, sec, nsec int64) {
	st.Ctimespec = syscall.Timespec{Sec: sec, Nsec: nsec}
}
