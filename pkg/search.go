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
	if searchParams.Start == "" {
		searchParams.Start = "1h"
	}
	if searchParams.LimitPerItem == 0 {
		searchParams.LimitPerItem = 100
	}
	if searchParams.Limit <= 0 {
		searchParams.Limit = 50
	}
	if searchParams.LimitBytesPerItem == 0 {
		searchParams.LimitBytesPerItem = 100 * 1024
	}
	timer := timer.NewTimer()
	results := &logs.SearchResults{}
	for _, backend := range logs.GlobalBackends {
		searchResult, err := backend.Backend.Search(searchParams)
		if err != nil {
			logger.Errorf("error executing error: %v", err)
			continue
		}
		results.Append(&searchResult)
	}
	logger.Infof("[%s] => %d results in %s", searchParams, results.Total, timer)
	return cc.JSON(http.StatusOK, *results)
}
