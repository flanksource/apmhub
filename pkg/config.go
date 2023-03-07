package pkg

import (
	"fmt"
	"os"
	"path/filepath"

	v8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/flanksource/flanksource-ui/apm-hub/api/logs"
	"github.com/flanksource/flanksource-ui/apm-hub/pkg/elasticsearch"
	"github.com/flanksource/flanksource-ui/apm-hub/pkg/files"
	k8s "github.com/flanksource/flanksource-ui/apm-hub/pkg/kubernetes"
	"github.com/flanksource/kommons"
	"gopkg.in/yaml.v3"
)

// ParseConfig parses the config file and returns the SearchConfig
func ParseConfig(configFile string) (*logs.SearchConfig, error) {
	searchConfig := &logs.SearchConfig{
		Path: configFile,
	}
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("error reading the configFile: %v", err)
	}

	if err := yaml.Unmarshal(data, searchConfig); err != nil {
		return nil, fmt.Errorf("error unmarshalling the configFile: %v", err)
	}

	return searchConfig, nil
}

// LoadBackendsFromConfig loads the backends from the config file
func LoadBackendsFromConfig(kommonsClient *kommons.Client, searchConfig *logs.SearchConfig) ([]logs.SearchBackend, error) {
	var backends []logs.SearchBackend
	for _, backend := range searchConfig.Backends {
		if backend.Kubernetes != nil {
			client, err := k8s.GetKubeClient(kommonsClient, backend.Kubernetes)
			if err != nil {
				return nil, fmt.Errorf("error getting the kube client: %w", err)
			}
			backend.Backend = &k8s.KubernetesSearch{
				Client: client,
			}
			backends = append(backends, backend)
		}

		if len(backend.Files) != 0 {
			// If the paths are not absolute,
			// They should be parsed with respect to the provided config file
			for i, f := range backend.Files {
				for j, p := range f.Paths {
					if !filepath.IsAbs(p) {
						backend.Files[i].Paths[j] = filepath.Join(filepath.Dir(searchConfig.Path), p)
					}
				}
			}

			backend.Backend = &files.FileSearch{
				FilesBackend: backend.Files,
			}
			backends = append(backends, backend)
		}

		if backend.ElasticSearch != nil {
			cfg, err := getElasticConfig(backend.ElasticSearch)
			if err != nil {
				return nil, fmt.Errorf("error getting the elastic search config: %w", err)
			}

			client, err := v8.NewClient(*cfg)
			if err != nil {
				return nil, fmt.Errorf("error creating the elastic search client: %w", err)
			}

			pingResp, err := client.Ping()
			if err != nil {
				return nil, fmt.Errorf("error pinging the elastic search client: %w", err)
			}

			if pingResp.StatusCode != 200 {
				return nil, fmt.Errorf("got ping response: %d", pingResp.StatusCode)
			}

			es, err := elasticsearch.NewElasticSearchBackend(client, backend.ElasticSearch.Index, backend.ElasticSearch.Query)
			if err != nil {
				return nil, fmt.Errorf("error creating the elastic search backend: %w", err)
			}
			backend.Backend = es

			backends = append(backends, backend)
		}
	}

	return backends, nil
}

func getElasticConfig(conf *logs.ElasticSearchBackend) (*v8.Config, error) {
	if conf.Address != "" && conf.CloudID != "" {
		return nil, fmt.Errorf("provide either an address or a cloudID")
	}

	cfg := v8.Config{
		Username: conf.Username,
		Password: conf.Password,
	}

	if conf.Address != "" {
		cfg.Addresses = []string{conf.Address}
	} else if conf.CloudID != "" {
		cfg.CloudID = conf.CloudID
		cfg.APIKey = conf.APIKey
	} else {
		return nil, fmt.Errorf("provide either an address or a cloudID")
	}

	return &cfg, nil
}
