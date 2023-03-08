package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
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

	config.Fields.ExclusionsRegexp = make([]*regexp.Regexp, len(config.Fields.Exclusions))
	for i, k := range config.Fields.Exclusions {
		regexp, err := regexp.Compile(k)
		if err != nil {
			return nil, fmt.Errorf("error compiling the regexp [%s]: %w", k, err)
		}

		config.Fields.ExclusionsRegexp[i] = regexp
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

	if err := t.template.Execute(&buf, q.Labels); err != nil {
		return result, fmt.Errorf("error executing template: %w", err)
	}
	logger.Debugf("Query: %s", string(buf.Bytes()))

	res, err := t.client.Search(
		t.client.Search.WithContext(context.Background()),
		t.client.Search.WithIndex(t.index),
		t.client.Search.WithBody(&buf),
	)
	if err != nil {
		return result, fmt.Errorf("error searching: %w", err)
	}
	defer res.Body.Close()

	var r map[string]any
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return result, fmt.Errorf("error parsing the response body: %w", err)
	}

	hits, ok := r["hits"].(map[string]any)
	if !ok {
		return result, nil
	}

	data, ok := hits["hits"].([]any)
	if !ok {
		return result, nil
	}

	result.Results = t.getResultsFromHits(data)
	result.Total = getTotalResultsCount(hits)
	result.NextPage = getNextPage(hits)
	return result, nil
}

func getNextPage(hits map[string]any) string {
	return ""
}

func getTotalResultsCount(hits map[string]any) int {
	total, ok := hits["total"].(map[string]interface{})
	if !ok {
		return 0
	}

	val, ok := total["value"].(float64)
	if !ok {
		return 0
	}

	return int(val)
}

func (t *OpenSearchBackend) getResultsFromHits(data []any) []logs.Result {
	resp := make([]logs.Result, 0, len(data))
	for _, v := range data {
		data, ok := v.(map[string]any)
		if !ok {
			logger.Debugf("invalid data type [%T]: %v", v, v)
			continue
		}

		var (
			id, _        = data["_id"].(string)
			source, _    = data["_source"].(map[string]any)
			timestamp, _ = source[t.fields.Timestamp].(string)
		)

		msgField, ok := source[t.fields.Message]
		if !ok {
			logger.Debugf("message field [%s] not found", t.fields.Message)
			continue
		}

		msg, err := stringify(msgField)
		if err != nil {
			logger.Debugf("error stringifying message: %v", err)
			continue
		}

		if shouldExclude(msg, t.fields.ExclusionsRegexp) {
			logger.Debugf("message excluded: %s", msg)
			continue
		}

		resp = append(resp, logs.Result{
			Id:      id,
			Message: msg,
			Time:    timestamp,
			Labels:  extractLabelsFromSource(source, t.fields.Labels),
		})
	}

	return resp
}

func extractLabelsFromSource(src map[string]any, fields []string) map[string]string {
	source := make(map[string]string)
	for k, v := range src {
		if collections.Contains(fields, k) {
			b, err := json.Marshal(v)
			if err != nil {
				continue
			}

			source[k] = string(b)
		}
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

func shouldExclude(msg string, exclusions []*regexp.Regexp) bool {
	for _, r := range exclusions {
		if r.MatchString(msg) {
			return true
		}
	}

	return false
}
