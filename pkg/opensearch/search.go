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
		t.client.Search.WithSize(int(q.Limit)),
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

	result.Results = t.getResultsFromHits(r.Hits.Hits)
	result.Total = int(r.Hits.Total.Value)
	result.NextPage = getNextPage(r.Hits.Hits)
	return result, nil
}

func getNextPage(rows []ElasticsearchHit) string {
	if len(rows) == 0 {
		return ""
	}

	lastItem := rows[len(rows)-1]
	val, err := stringify(lastItem.Sort)
	if err != nil {
		logger.Errorf("error stringifying sort: %v", err)
		return ""
	}

	return val
}

func (t *OpenSearchBackend) getResultsFromHits(rows []ElasticsearchHit) []logs.Result {
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

		var timestamp, _ = row.Source[t.fields.Timestamp].(string)
		resp = append(resp, logs.Result{
			Id:      row.ID,
			Message: msg,
			Time:    timestamp,
			Labels:  t.extractLabelsFromSource(row.Source, t.fields.Exclusions),
		})
	}

	return resp
}

func (t *OpenSearchBackend) extractLabelsFromSource(src map[string]any, fields []string) map[string]string {
	source := make(map[string]string)
	for k, v := range src {
		// Exclude message field, timestamp field and fields that are explicitly excluded
		if k == t.fields.Message || k == t.fields.Timestamp || collections.Contains(fields, k) {
			continue
		}

		b, err := stringify(v)
		if err != nil {
			logger.Errorf("failed to stringify field %s: %v", k, err)
			continue
		}

		source[k] = b
	}

	return source
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
