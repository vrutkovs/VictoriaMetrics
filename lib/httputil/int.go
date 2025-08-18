package httputil

import (
	"fmt"
	"net/http"
	"strconv"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// GetInt returns integer value from the given argKey.
func GetInt(r *http.Request, argKey string) (int, error) {
	span := logger.TraceRequest(r)
	defer span.End()


	argValue := r.FormValue(argKey)
	if len(argValue) == 0 {
		return 0, nil
	}
	n, err := strconv.Atoi(argValue)
	if err != nil {
		return 0, fmt.Errorf("cannot parse integer %q=%q: %w", argKey, argValue, err)
	}
	return n, nil
}
