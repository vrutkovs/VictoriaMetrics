package netstorage

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// recvBlock mimics processBlocks.f + ProcessSearchQuery.processBlock (913fa97567).
func recvBlock(tbfw *tmpBlocksFileWrapper, mb *storage.MetricBlock, raw []byte, workerID uint) error {
	if _, err := mb.Unmarshal(raw); err != nil {
		return err
	}
	return tbfw.RegisterAndWriteBlock(mb, workerID)
}
