package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"text/template"

	"github.com/flanksource/commons/collections"
	"github.com/flanksource/commons/logger"
	"github.com/flanksource/flanksource-ui/apm-hub/api/logs"
	"github.com/jeremywohl/flatten"
	opensearch "github.com/opensearch-project/opensearch-go/v2"
)

type OpenSearchBackend struct {
	client   *opensearch.Client
	fields   logs.ElasticSearchFields
	template *template.Template
	index    string
}

func NewOpenSearchBackend(client *opensearch.Client, config *logs.OpenSearchBackend) (*OpenSearchBackend, error) {
	if client == nil {
		return nil, fmt.Errorf("client is nil")
	}

	if config.Index == "" {
		return nil, fmt.Errorf("index is empty")
	}

	template, err := template.New("query").Parse(config.Query)
	if err != nil {
		return nil, err
	}

	return &OpenSearchBackend{
		fields:   config.Fields,
		client:   client,
		index:    config.Index,
		template: template,
	}, nil
}

func (t *OpenSearchBackend) Search(q *logs.SearchParams) (logs.SearchResults, error) {
	var result logs.SearchResults
	var buf bytes.Buffer

	if err := t.template.Execute(&buf, q); err != nil {
		return result, fmt.Errorf("error executing template: %w", err)
	}
	logger.Debugf("Query: %s", string(buf.Bytes()))

	res, err := t.client.Search(
		t.client.Search.WithContext(context.Background()),
		t.client.Search.WithIndex(t.index),
		t.client.Search.WithBody(&buf),
		t.client.Search.WithSize(int(q.Limit+1)),
		t.client.Search.WithErrorTrace(),
	)
	if err != nil {
		return result, fmt.Errorf("error searching: %w", err)
	}
	defer res.Body.Close()

	var r OpenSearchResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return result, fmt.Errorf("error parsing the response body: %w", err)
	}

	result.Results = t.getResultsFromHits(q.Limit, r.Hits.Hits)
	result.Total = int(r.Hits.Total.Value)
	result.NextPage = getNextPage(int(q.Limit), r.Hits.Hits)
	return result, nil
}

func getNextPage(requestedRowsCount int, rows []ElasticsearchHit) string {
	if len(rows) == 0 {
		return ""
	}

	// If we got less than the requested rows count, we are at the end of the results.
	// Note: We always request one more than the requested rows count, so we can
	// determine if there are more results to fetch.
	if requestedRowsCount >= len(rows) {
		return ""
	}

	lastItem := rows[len(rows)-2]
	val, err := stringify(lastItem.Sort)
	if err != nil {
		logger.Errorf("error stringifying sort: %v", err)
		return ""
	}

	return val
}

func (t *OpenSearchBackend) getResultsFromHits(requestedRowsCount int64, rows []ElasticsearchHit) []logs.Result {
	if len(rows) > int(requestedRowsCount) {
		rows = rows[:requestedRowsCount]
	}

	resp := make([]logs.Result, 0, len(rows))
	for _, row := range rows {
		msgField, ok := row.Source[t.fields.Message]
		if !ok {
			logger.Debugf("message field [%s] not found", t.fields.Message)
			continue
		}

		msg, err := stringify(msgField)
		if err != nil {
			logger.Debugf("error stringifying message: %v", err)
			continue
		}

		labels, err := t.extractLabelsFromSource(row.Source, t.fields.Exclusions)
		if err != nil {
			logger.Errorf("error extracting labels: %v", err)
		}

		var timestamp, _ = row.Source[t.fields.Timestamp].(string)
		resp = append(resp, logs.Result{
			Id:      row.ID,
			Message: msg,
			Time:    timestamp,
			Labels:  labels,
		})
	}

	return resp
}

func (t *OpenSearchBackend) extractLabelsFromSource(src map[string]any, fields []string) (map[string]string, error) {
	sourceAfterExclusion := make(map[string]any)
	for k, v := range src {
		// Exclude message field, timestamp field and fields that are explicitly excluded
		if k == t.fields.Message || k == t.fields.Timestamp || collections.Contains(fields, k) {
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
		str, err := stringify(v)
		if err != nil {
			logger.Errorf("error stringifying %v: %v", v, err)
			continue
		}

		stringedLabels[k] = str
	}

	return stringedLabels, nil
}

func stringify(val any) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}
