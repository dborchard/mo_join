package process

import "mojoins/pkg/common/mpool"

func (wreg *WaitRegister) CleanChannel(m *mpool.MPool) {
	for len(wreg.Ch) > 0 {
		bat := <-wreg.Ch
		if bat != nil {
			bat.Clean(m)
		}
	}
}
