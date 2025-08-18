package promremotewrite

import (
	"context"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/promremotewrite/stream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted  = metrics.NewCounter(`vm_rows_inserted_total{type="promremotewrite"}`)
	rowsPerInsert = metrics.NewHistogram(`vm_rows_per_insert{type="promremotewrite"}`)
)

// InsertHandler processes remote write for prometheus.
func InsertHandler(req *http.Request) error {
	span := logger.TraceRequest(req)
	defer span.End()
	extraLabels, err := protoparserutil.GetExtraLabels(req)
	if err != nil {
		return err
	}
	isVMRemoteWrite := req.Header.Get("Content-Encoding") == "zstd"
	ctx := logger.GetCtxFromRequest(req)
	return stream.Parse(ctx, req.Body, isVMRemoteWrite, func(tss []prompb.TimeSeries, _ []prompb.MetricMetadata) error {
		return insertRows(ctx, tss, extraLabels)
	})
}

func insertRows(spanCtx context.Context, timeseries []prompb.TimeSeries, extraLabels []prompb.Label) error {
	_, span := logger.Trace(spanCtx)
	defer span.End()

	ctx := common.GetInsertCtx()
	defer common.PutInsertCtx(ctx)

	rowsLen := 0
	for i := range timeseries {
		rowsLen += len(timeseries[i].Samples)
	}
	ctx.Reset(rowsLen)
	rowsTotal := 0
	hasRelabeling := relabel.HasRelabeling()
	for i := range timeseries {
		ts := &timeseries[i]
		rowsTotal += len(ts.Samples)
		ctx.Labels = ctx.Labels[:0]
		srcLabels := ts.Labels
		for _, srcLabel := range srcLabels {
			ctx.AddLabel(srcLabel.Name, srcLabel.Value)
		}
		for j := range extraLabels {
			label := &extraLabels[j]
			ctx.AddLabel(label.Name, label.Value)
		}

		if !ctx.TryPrepareLabels(hasRelabeling) {
			continue
		}
		var metricNameRaw []byte
		var err error
		samples := ts.Samples
		for i := range samples {
			r := &samples[i]

			_, subspan := logger.ChildSpan(spanCtx, "promremotewrite.WriteDataPointExt")
			metricNameRaw, err = ctx.WriteDataPointExt(metricNameRaw, ctx.Labels, r.Timestamp, r.Value)
			if err != nil {
				subspan.RecordError(err)
				subspan.End()
				return err
			}
			subspan.End()
		}
	}
	rowsInserted.Add(rowsTotal)
	rowsPerInsert.Update(float64(rowsTotal))

	_, subspan := logger.ChildSpan(spanCtx, "promremotewrite.FlushBufs")
	res := ctx.FlushBufs()
	subspan.End()
	return res
}
