package pkg

import (
	"fmt"
	"os"
	"path/filepath"

	v8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/flanksource/apm-hub/api/logs"
	"github.com/flanksource/apm-hub/db"
	"github.com/flanksource/apm-hub/pkg/elasticsearch"
	"github.com/flanksource/apm-hub/pkg/files"
	k8s "github.com/flanksource/apm-hub/pkg/kubernetes"
	pkgOpensearch "github.com/flanksource/apm-hub/pkg/opensearch"
	"github.com/flanksource/commons/logger"
	"github.com/flanksource/kommons"
	"github.com/opensearch-project/opensearch-go/v2"
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

// SetupBackends loads the backends from the config file
func SetupBackends(kommonsClient *kommons.Client, configBackends []logs.SearchBackend) ([]logs.SearchBackend, error) {
	var backends []logs.SearchBackend
	for _, backend := range configBackends {
		if err := AttachSearchAPIToBackend(kommonsClient, &backend); err != nil {
			logger.Errorf("Error attaching search api backend: %v", err)
		}
		backends = append(backends, backend)
	}
	return backends, nil
}

func LoadGlobalBackends() error {
	kommonsClient, err := kommons.NewClientFromDefaults(logger.GetZapLogger())
	if err != nil {
		return err
	}

	dbBackends, err := db.GetLoggingBackends()
	if err != nil {
		return err
	}
	backends, err := SetupBackends(kommonsClient, dbBackends)
	if err != nil {
		return err
	}

	logs.GlobalBackends = backends
	return nil
}

func AttachSearchAPIToBackend(kommonsClient *kommons.Client, backend *logs.SearchBackend) error {
	if backend.Kubernetes != nil {
		k8sclient, err := k8s.GetKubeClient(kommonsClient, backend.Kubernetes)
		if err != nil {
			return err
		}
		backend.API = &k8s.KubernetesSearch{
			Client: k8sclient,
		}
	}

	if len(backend.Files) > 0 {
		// If the paths are not absolute,
		// They should be parsed with respect to the current path
		for i, f := range backend.Files {
			for j, p := range f.Paths {
				if !filepath.IsAbs(p) {
					currentPath, _ := os.Getwd()
					backend.Files[i].Paths[j] = filepath.Join(currentPath, p)
				}
			}
		}

		backend.API = &files.FileSearch{
			FilesBackendConfig: backend.Files,
		}
	}

	if backend.ElasticSearch != nil {
		cfg, err := getElasticConfig(kommonsClient, backend.ElasticSearch)
		if err != nil {
			return fmt.Errorf("error getting the elastic search config: %w", err)
		}

		esClient, err := v8.NewClient(*cfg)
		if err != nil {
			return fmt.Errorf("error creating the elastic search client: %w", err)
		}

		pingResp, err := esClient.Ping()
		if err != nil {
			return fmt.Errorf("error pinging the elastic search client: %w", err)
		}

		if pingResp.StatusCode != 200 {
			return fmt.Errorf("[elasticsearch] got ping response: %d", pingResp.StatusCode)
		}

		es, err := elasticsearch.NewElasticSearchBackend(esClient, backend.ElasticSearch)
		if err != nil {
			return fmt.Errorf("error creating the elastic search backend: %w", err)
		}
		backend.API = es
	}

	if backend.OpenSearch != nil {
		cfg, err := getOpenSearchConfig(kommonsClient, backend.OpenSearch)
		if err != nil {
			return fmt.Errorf("error getting the openSearch config: %w", err)
		}

		osClient, err := opensearch.NewClient(*cfg)
		if err != nil {
			return fmt.Errorf("error creating the openSearch client: %w", err)
		}

		pingResp, err := osClient.Ping()
		if err != nil {
			return fmt.Errorf("error pinging the openSearch client: %w", err)
		}

		if pingResp.StatusCode != 200 {
			return fmt.Errorf("[opensearch] got ping response: %d", pingResp.StatusCode)
		}

		osBackend, err := pkgOpensearch.NewOpenSearchBackend(osClient, backend.OpenSearch)
		if err != nil {
			return fmt.Errorf("error creating the openSearch backend: %w", err)
		}
		backend.API = osBackend
	}

	return nil
}

func getOpenSearchEnvVars(client *kommons.Client, conf *logs.OpenSearchBackendConfig) (username, password string, err error) {
	if conf.Username != nil {
		_, username, err = client.GetEnvValue(*conf.Username, conf.Namespace)
		if err != nil {
			err = fmt.Errorf("error getting the username: %w", err)
			return
		}
	}

	if conf.Password != nil {
		_, password, err = client.GetEnvValue(*conf.Password, conf.Namespace)
		if err != nil {
			err = fmt.Errorf("error getting the password: %w", err)
			return
		}
	}

	return
}

func getElasticSearchEnvVars(kClient *kommons.Client, conf *logs.ElasticSearchBackendConfig) (cloudID, apiKey, username, password string, err error) {
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

func getElasticConfig(kClient *kommons.Client, conf *logs.ElasticSearchBackendConfig) (*v8.Config, error) {
	cloudID, apiKey, username, password, err := getElasticSearchEnvVars(kClient, conf)
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
		return nil, fmt.Errorf("provide at least an address or a cloudID")
	}

	return &cfg, nil
}

func getOpenSearchConfig(kClient *kommons.Client, conf *logs.OpenSearchBackendConfig) (*opensearch.Config, error) {
	username, password, err := getOpenSearchEnvVars(kClient, conf)
	if err != nil {
		return nil, fmt.Errorf("error getting the env vars: %w", err)
	}

	if conf.Address == "" {
		return nil, fmt.Errorf("address is required for OpenSearch")
	}

	cfg := opensearch.Config{
		Username:  username,
		Password:  password,
		Addresses: []string{conf.Address},
	}

	return &cfg, nil
}
