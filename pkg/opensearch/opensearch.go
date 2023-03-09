package opensearch

type TotalHitsInfo struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

type HitsInfo struct {
	Total    TotalHitsInfo      `json:"total"`
	MaxScore float64            `json:"max_score"`
	Hits     []ElasticsearchHit `json:"hits"`
}

type OpenSearchResponse struct {
	Took     float64 `json:"took"`
	TimedOut bool    `json:"timed_out"`
	Hits     HitsInfo
}

type ElasticsearchHit struct {
	Index  string         `json:"_index"`
	Type   string         `json:"_type"`
	ID     string         `json:"_id"`
	Score  float64        `json:"_score"`
	Sort   []any          `json:"sort"`
	Source map[string]any `json:"_source"`
}
