package netstorage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// recvBlock mimics the vmstorage RPC callback: the raw MetricBlock goes to the tmp blocks file.
func recvBlock(tbfw *tmpBlocksFileWrapper, _ *storage.MetricBlock, raw []byte, workerID uint) error {
	metricName, block, _, err := storage.SplitMetricBlock(raw)
	if err != nil {
		return fmt.Errorf("cannot split MetricBlock: %w", err)
	}
	return tbfw.RegisterAndWriteBlock(metricName, block, workerID)
}
