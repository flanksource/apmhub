//go:build integration

package files_test

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/flanksource/apm-hub/api/logs"
	"github.com/flanksource/apm-hub/cmd"
	"github.com/flanksource/apm-hub/pkg"
	"github.com/flanksource/apm-hub/pkg/files"
)

func TestFileSearch(t *testing.T) {
	testData := []struct {
		Name       string
		Labels     map[string]string // Labels passed to the search
		MatchFiles []string          // MatchFiles contains the list of files that'll be read directly to compare against the search result.
	}{
		{
			Name: "simple",
			Labels: map[string]string{
				"name": "acmehost",
				"type": "Nginx",
			},
			MatchFiles: []string{"../../samples/nginx-access.log"},
		},
		{
			Name: "glob",
			Labels: map[string]string{
				"name": "all",
				"type": "Nginx",
			},
			MatchFiles: []string{"../../samples/nginx-access.log", "../../samples/nginx-error.log"},
		},
	}

	confPath := "../../samples/config-file.yaml"
	backend, err := pkg.ParseConfig(nil, confPath)
	if err != nil {
		t.Fatal("Fail to parse the config file", err)
	}
	logs.GlobalBackends = append(logs.GlobalBackends, backend...)

	for i, td := range testData {
		t.Run(td.Name, func(t *testing.T) {
			sp := logs.SearchParams{Labels: td.Labels}
			b, err := json.Marshal(sp)
			if err != nil {
				t.Fatal("Failed to marshal search param")
			}

			req := httptest.NewRequest(http.MethodPost, "/search", strings.NewReader(string(b)))
			req.Header.Add("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			e := cmd.SetupServer(nil)
			e.ServeHTTP(rec, req)

			var res logs.SearchResults
			if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
				t.Fatal("Failed to decode the search result")
			}

			// Directly read those files to compare it against the search result
			lines := readLines(t, td.MatchFiles)

			if len(res.Results) != len(lines) {
				t.Fatalf("[%d] Expected [%d] lines but got [%d]", i, len(lines), len(res.Results))
			}

			for i, r := range res.Results {
				if r.Message != lines[i] {
					t.Fatalf("[%d] Incorrect line [%d]. Expected %s got %s", i, i+1, lines[i], r.Message)
				}

				matchLabels(t, r.Labels, td.Labels, td.MatchFiles)
			}
		})
	}
}

// matchLabels tries to match the label returned from the search result
// against many labels by iterating through the given paths.
func matchLabels(t *testing.T, labels, searchLabels map[string]string, paths []string) {
	t.Helper()

	for _, path := range paths {
		expectedLabel := files.MergeMap(searchLabels, map[string]string{"path": path})
		if reflect.DeepEqual(labels, expectedLabel) {
			return
		}
	}

	t.Fatalf("Incorrect label. Got [%v]\n", labels)
}

// readLines is a helper func to read the file lines
func readLines(t *testing.T, paths []string) []string {
	t.Helper()

	var lines []string
	for _, filePath := range paths {
		nginxLogFile, err := os.Open(filePath)
		if err != nil {
			t.Fatal("Fail to read nginx log", err)
		}

		scanner := bufio.NewScanner(nginxLogFile)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
	}

	return lines
}
