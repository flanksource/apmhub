package pkg

import (
	"net/http"

	"github.com/flanksource/commons/logger"
	"github.com/flanksource/commons/timer"

	"github.com/flanksource/flanksource-ui/apm-hub/api"
	"github.com/flanksource/flanksource-ui/apm-hub/api/logs"
	"github.com/labstack/echo/v4"
)

// Search and collate logs
func Search(c echo.Context) error {
	cc := c.(*api.Context)
	searchParams := new(logs.SearchParams)
	err := c.Bind(searchParams)
	if err != nil {
		cc.Error(err)
	}
	searchParams.SetDefaults()

	timer := timer.NewTimer()
	results := &logs.SearchResults{}
	for i, backend := range logs.GlobalBackends {
		matched, isAdditive := backend.Backend.MatchRoute(searchParams)
		if !matched {
			logger.Debugf("backend[%d] did not match any routes", i)
			continue
		}

		searchResult, err := backend.Backend.Search(searchParams)
		if err != nil {
			logger.Errorf("error searching backend[%d]: %v", i, err)
			continue
		}
		results.Append(&searchResult)

		// If the route is additive, all the previous search results are discarded
		// and just the search result from this backend is returned exclusively.
		if isAdditive {
			logger.Infof("additive route matched. discarding previous results and exiting early")
			results := &logs.SearchResults{}
			results.Append(&searchResult)
			break
		}
	}

	logger.Infof("[%s] => %d results in %s", searchParams, results.Total, timer)

	return cc.JSON(http.StatusOK, *results)
}
