package httputil

import (
	"net/http"
	"strings"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// GetBool returns boolean value from the given argKey query arg.
func GetBool(r *http.Request, argKey string) bool {
	span := logger.TraceRequest(r)
	defer span.End()


	argValue := r.FormValue(argKey)
	switch strings.ToLower(argValue) {
	case "", "0", "f", "false", "no":
		return false
	default:
		return true
	}
}
