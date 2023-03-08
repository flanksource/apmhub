package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"text/template"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/flanksource/commons/logger"
	"github.com/flanksource/flanksource-ui/apm-hub/api/logs"
)

type ElasticSearchBackend struct {
	client   *elasticsearch.Client
	template *template.Template
	index    string
}

func NewElasticSearchBackend(client *elasticsearch.Client, index, tpl string) (*ElasticSearchBackend, error) {
	logger.Infof("Parsing template: %s", tpl)

	template, err := template.New("query").Parse(tpl)
	if err != nil {
		return nil, err
	}

	return &ElasticSearchBackend{
		client:   client,
		index:    index,
		template: template,
	}, nil
}

func (t *ElasticSearchBackend) Search(q *logs.SearchParams) (logs.SearchResults, error) {
	var result logs.SearchResults
	var buf bytes.Buffer

	if err := t.template.Execute(&buf, q.Labels); err != nil {
		return result, fmt.Errorf("error executing template: %w", err)
	}

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

	result.Results = getResultsFromHits(data)
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

func getResultsFromHits(data []any) []logs.Result {
	resp := make([]logs.Result, 0, len(data))
	for _, v := range data {
		data, ok := v.(map[string]any)
		if !ok {
			logger.Debugf("invalid data type [%T]: %v", v, v)
			continue
		}

		var (
			id, _     = data["_id"].(string)
			idx, _    = data["_index"].(string)
			source, _ = data["_source"].(map[string]any)
		)

		b, err := json.Marshal(source)
		if err != nil {
			logger.Errorf("error marshalling source: %v", err)
			continue
		}

		resp = append(resp, logs.Result{
			Id:      id,
			Message: string(b),
			Labels: map[string]string{
				"index": idx,
			},
		})
	}

	return resp
}
