package elasticsearch

import (
	"fmt"

	"github.com/flanksource/apm-hub/api/logs"
	"github.com/flanksource/commons/collections"
	"github.com/flanksource/commons/logger"
	"github.com/flanksource/commons/utils"
	"github.com/jeremywohl/flatten"
)

type TotalHitsInfo struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

type HitsInfo struct {
	Total    TotalHitsInfo `json:"total"`
	MaxScore float64       `json:"max_score"`
	Hits     []SearchHit   `json:"hits"`
}

type SearchResponse struct {
	Took     float64 `json:"took"`
	TimedOut bool    `json:"timed_out"`
	Hits     HitsInfo
}

type SearchHit struct {
	Index  string         `json:"_index"`
	Type   string         `json:"_type"`
	ID     string         `json:"_id"`
	Score  float64        `json:"_score"`
	Sort   []any          `json:"sort"`
	Source map[string]any `json:"_source"`
}

// NextPage returns the next page token.
func (t *HitsInfo) NextPage(requestedRowsCount int) string {
	if len(t.Hits) == 0 {
		return ""
	}

	// If we got less than the requested rows count, we are at the end of the results.
	// Note: We always request one more than the requested rows count, so we can
	// determine if there are more results to fetch.
	if requestedRowsCount >= len(t.Hits) {
		return ""
	}

	lastItem := t.Hits[len(t.Hits)-2]
	val, err := utils.Stringify(lastItem.Sort)
	if err != nil {
		logger.Errorf("error stringifying sort: %v", err)
		return ""
	}

	return val
}

// GetResultsFromHits returns the results from the hits.
func (t *HitsInfo) GetResultsFromHits(requestedRowsCount int64, msgField, timestampField string, excludeFields ...string) []logs.Result {
	// Don't user more than the requested rows count.
	rows := t.Hits
	if len(t.Hits) > int(requestedRowsCount) {
		rows = t.Hits[:requestedRowsCount]
	}

	resp := make([]logs.Result, 0, len(rows))
	for _, row := range rows {
		msgVal, ok := row.Source[msgField]
		if !ok {
			logger.Debugf("message field [%s] not found", msgField)
			continue
		}

		msg, err := utils.Stringify(msgVal)
		if err != nil {
			logger.Debugf("error stringifying message: %v", err)
			continue
		}

		labels, err := extractLabelsFromSource(row.Source, msgField, timestampField, excludeFields...)
		if err != nil {
			logger.Errorf("error extracting labels: %v", err)
		}

		var timestamp, _ = row.Source[timestampField].(string)
		resp = append(resp, logs.Result{
			Id:      row.ID,
			Message: msg,
			Time:    timestamp,
			Labels:  labels,
		})
	}

	return resp
}

// extractLabelsFromSource extracts labels from the source, excluding the message field, timestamp field
// and fields that are explicitly excluded.
func extractLabelsFromSource(src map[string]any, msgField, timestampField string, fields ...string) (map[string]string, error) {
	sourceAfterExclusion := make(map[string]any)
	for k, v := range src {
		// Exclude message field, timestamp field and fields that are explicitly excluded
		if k == msgField || k == timestampField || collections.Contains(fields, k) {
			continue
		}

		sourceAfterExclusion[k] = v
	}

	flattenedLabels, err := flatten.Flatten(sourceAfterExclusion, "", flatten.DotStyle)
	if err != nil {
		return nil, fmt.Errorf("error flattening source: %w", err)
	}

	stringedLabels := make(map[string]string, len(flattenedLabels))
	for k, v := range flattenedLabels {
		str, err := utils.Stringify(v)
		if err != nil {
			logger.Errorf("error stringifying %v: %v", v, err)
			continue
		}

		stringedLabels[k] = str
	}

	return stringedLabels, nil
}
