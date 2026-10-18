package files

import "syscall"

// changeTime returns the inode change time of st, which Linux keeps in Ctim.
func changeTime(st *syscall.Stat_t) (int64, int64) {
	return st.Ctim.Sec, st.Ctim.Nsec
}
