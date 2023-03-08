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
			cfg, err := getElasticConfig(kommonsClient, backend.ElasticSearch)
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

func getEnvVars(kClient *kommons.Client, conf *logs.ElasticSearchBackend) (cloudID, apiKey, username, password string, err error) {
	if conf.CloudID != nil {
		_, cloudID, err = kClient.GetEnvValue(*conf.CloudID, conf.Namespace)
		if err != nil {
			err = fmt.Errorf("error getting the cloudID: %w", err)
			return
		}
	}

	if conf.Username != nil {
		_, username, err = kClient.GetEnvValue(*conf.Username, conf.Namespace)
		if err != nil {
			err = fmt.Errorf("error getting the username: %w", err)
			return
		}
	}

	if conf.Password != nil {
		_, password, err = kClient.GetEnvValue(*conf.Password, conf.Namespace)
		if err != nil {
			err = fmt.Errorf("error getting the password: %w", err)
			return
		}
	}

	if conf.APIKey != nil {
		_, apiKey, err = kClient.GetEnvValue(*conf.APIKey, conf.Namespace)
		if err != nil {
			err = fmt.Errorf("error getting the apiKey: %w", err)
			return
		}
	}

	return
}

func getElasticConfig(kClient *kommons.Client, conf *logs.ElasticSearchBackend) (*v8.Config, error) {
	cloudID, apiKey, username, password, err := getEnvVars(kClient, conf)
	if err != nil {
		return nil, fmt.Errorf("error getting the env vars: %w", err)
	}

	if conf.Address != "" && cloudID != "" {
		return nil, fmt.Errorf("provide either an address or a cloudID")
	}

	cfg := v8.Config{
		Username: username,
		Password: password,
	}

	if conf.Address != "" {
		cfg.Addresses = []string{conf.Address}
	} else if cloudID != "" {
		cfg.CloudID = cloudID
		cfg.APIKey = apiKey
	} else {
		return nil, fmt.Errorf("provide either an address or a cloudID")
	}

	return &cfg, nil
}
