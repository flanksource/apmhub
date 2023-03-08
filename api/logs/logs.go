package logs

import (
	"bufio"
	"fmt"
	"strings"
	"time"

	"github.com/flanksource/kommons"
)

var GlobalBackends []SearchBackend

type SearchConfig struct {
	// Path is the path of this config file
	Path     string          `yaml:"-"`
	Backends []SearchBackend `yaml:"backends,omitempty"`
}

type SearchBackend struct {
	Routes        []SearchRoute            `json:"routes,omitempty"`
	Backend       SearchAPI                `json:"-"`
	ElasticSearch *ElasticSearchBackend    `json:"elasticsearch,omitempty"`
	Kubernetes    *KubernetesSearchBackend `json:"kubernetes,omitempty"`
	Files         []FileSearchBackend      `json:"file,omitempty" yaml:"file,omitempty"`
}

type SearchRoute struct {
	Type     string            `json:"type,omitempty"`
	IdPrefix string            `json:"idPrefix,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
}

type KubernetesSearchBackend struct {
	// empty kubeconfig indicates to use the current kubeconfig for connection
	Kubeconfig *kommons.EnvVar `json:"kubeconfig,omitempty"`
	//namespace to search the kommons.EnvVar in
	Namespace string `json:"namespace,omitempty"`
}

type FileSearchBackend struct {
	Labels map[string]string `yaml:"labels,omitempty"`
	Paths  []string          `yaml:"path,omitempty"`
}

type ElasticSearchBackend struct {
	Address   string `yaml:"address,omitempty"`
	Query     string `yaml:"query,omitempty"`
	Index     string `yaml:"index,omitempty"`
	Namespace string `json:"namespace,omitempty"` // Namespace to search the kommons.EnvVar in

	CloudID  *kommons.EnvVar `yaml:"cloudID,omitempty"`
	APIKey   *kommons.EnvVar `yaml:"apiKey,omitempty"`
	Username *kommons.EnvVar `yaml:"username,omitempty"`
	Password *kommons.EnvVar `yaml:"password,omitempty"`
}

type SearchParams struct {
	// Limit is the maximum number of results to return.
	Limit      int64 `json:"limit,omitempty"`
	LimitBytes int64 `json:"limitBytes,omitempty"`
	// The page token, returned by a previous call, to request the next page of results.
	Page string `json:"page,omitempty"`
	// comma separated list of labels to filter the results. key1=value1,key2=value2
	Labels map[string]string `json:"labels,omitempty"`
	// A generic query string, that is rewritten to the underlying system,
	// If the underlying system does not support queries, than this query is applied on the returned results
	Query string `json:"query,omitempty"`
	// A RFC3339 timestamp or an age string (e.g. "1h", "2d", "1w"), default to 1h
	Start string `json:"start,omitempty"`
	// A RFC3339 timestamp or an age string (e.g. "1h", "2d", "1w")
	End string `json:"end,omitempty"`
	// The type of logs to find, e.g. KubernetesNode, KubernetesService, KubernetesPod, VM, etc. Type and ID are used to route search requests
	Type string `json:"type,omitempty"`
	// The identifier of the type of logs to find, e.g. k8s-node-1, k8s-service-1, k8s-pod-1, vm-1, etc.
	// The ID should include include any cluster/namespace/account information required for routing
	Id string `json:"id,omitempty"`

	// Limits the number of log messages return per item, e.g. pod
	LimitPerItem int64 `json:"limitPerItem,omitempty"`
	// Limits the number of bytes returned per item, e.g. pod
	LimitBytesPerItem int64 `json:"limitBytesPerItem,omitempty"`

	start *time.Time `json:"-"`
	end   *time.Time `json:"-"`
}

func (p SearchParams) GetStart() *time.Time {
	if p.start != nil {
		return p.start
	}
	if duration, err := time.ParseDuration(p.Start); err == nil {
		t := time.Now().Add(-duration)
		p.start = &t
	} else if t, err := time.Parse(time.RFC3339, p.Start); err == nil {
		p.start = &t
	}
	return p.start
}

func (p SearchParams) GetEnd() *time.Time {
	if p.end != nil {
		return p.end
	}
	if duration, err := time.ParseDuration(p.End); err == nil {
		t := time.Now().Add(-duration)
		p.end = &t
	} else if t, err := time.Parse(time.RFC3339, p.End); err == nil {
		p.end = &t
	}
	return p.start
}

func (q SearchParams) String() string {
	s := ""
	if q.Type != "" {
		s += fmt.Sprintf("type=%s ", q.Type)
	}
	if q.Id != "" {
		s += fmt.Sprintf("id=%s ", q.Id)
	}
	if q.Start != "" {
		s += fmt.Sprintf("start=%s ", q.Start)
	}
	if q.Query != "" {
		s += fmt.Sprintf("query=%s ", q.Query)
	}
	if q.Labels != nil && len(q.Labels) > 0 {
		s += fmt.Sprintf("labels=%v ", q.Labels)
	}
	if q.End != "" {
		s += fmt.Sprintf("end=%s ", q.End)
	}
	if q.Limit > 0 {
		s += fmt.Sprintf("limit=%d ", q.Limit)
	}
	if q.Page != "" {
		s += fmt.Sprintf("page=%s ", q.Page)
	}
	return s
}

type SearchResults struct {
	Total    int      `json:"total,omitempty"`
	Results  []Result `json:"results,omitempty"`
	NextPage string   `json:"nextPage,omitempty"`
}

func (r *SearchResults) Append(other *SearchResults) {
	r.Results = append(r.Results, other.Results...)
	r.Total += other.Total
}

type Result struct {
	// Id is the unique identifier provided by the underlying system, use to link to a point in time of a log stream
	Id string `json:"id,omitempty"`
	// RFC3339 timestamp
	Time    string            `json:"timestamp,omitempty"`
	Message string            `json:"message,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}

func (r Result) Process() Result {
	scanner := bufio.NewScanner(strings.NewReader(r.Message))
	scanner.Split(bufio.ScanWords)
	if scanner.Scan() {
		timestamp := scanner.Text()
		if _, err := time.Parse(time.RFC3339, timestamp); err == nil {
			r.Time = timestamp
			r.Message = strings.ReplaceAll(r.Message, timestamp, "")
		}
	}
	r.Message = strings.TrimSpace(r.Message)
	return r
}

type SearchAPI interface {
	Search(q *SearchParams) (r SearchResults, err error)
}

type SearchMapper interface {
	MapSearchParams(p *SearchParams) ([]SearchParams, error)
}
