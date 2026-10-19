package files

import "syscall"

// changeTime returns the inode change time of st, which Darwin keeps in Ctimespec.
func changeTime(st *syscall.Stat_t) (int64, int64) {
	return st.Ctimespec.Sec, st.Ctimespec.Nsec
}
