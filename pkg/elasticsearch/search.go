package elasticsearch

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/flanksource/flanksource-ui/apm-hub/api/logs"
)

type ElasticSearchBackend struct {
	client *elasticsearch.Client
}

func NewElasticSearchBackend(client *elasticsearch.Client) *ElasticSearchBackend {
	return &ElasticSearchBackend{
		client: client,
	}
}

func (t *ElasticSearchBackend) Search(q *logs.SearchParams) (logs.SearchResults, error) {
	var result logs.SearchResults

	// result.Total = 112
	// result.NextPage = ""
	return result, nil
}
