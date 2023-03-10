package cmd

import (
	"net/http"
	"strconv"

	"github.com/flanksource/commons/logger"
	"github.com/flanksource/flanksource-ui/apm-hub/api"
	"github.com/flanksource/flanksource-ui/apm-hub/api/logs"
	"github.com/flanksource/flanksource-ui/apm-hub/pkg"
	"github.com/flanksource/kommons"
	"github.com/spf13/cobra"

	"github.com/labstack/echo/v4"
)

var Serve = &cobra.Command{
	Use:   "serve config.yaml",
	Short: "Start the for querying the logs",
	Run:   runServe,
}

func runServe(cmd *cobra.Command, configFiles []string) {
	kommonsClient, err := kommons.NewClientFromDefaults(logger.GetZapLogger())
	if err != nil {
		logger.Warnf("error getting the client from default k8s cluster: %v", err)
	}

	if len(configFiles) != 0 {
		for _, configFile := range configFiles {
			logger.Debugf("parsing config file: %s", configFile)
			config, err := pkg.ParseConfig(configFile)
			if err != nil {
				logger.Errorf("error parsing the configFile: %v", err)
				continue
			}

			logger.Debugf("loading backends from config file: %s", configFile)
			backends, err := pkg.LoadBackendsFromConfig(kommonsClient, config)
			if err != nil {
				logger.Errorf("error loading backends from the configFile: %v", err)
				continue
			}

			logger.Debugf("loaded %d backends from %s", len(backends), configFile)
			logs.GlobalBackends = append(logs.GlobalBackends, backends...)
		}
	}
	logger.Infof("loaded %d backends in total", len(logs.GlobalBackends))

	server := SetupServer(kommonsClient)
	addr := "0.0.0.0:" + strconv.Itoa(httpPort)
	server.Logger.Fatal(server.Start(addr))
}

func SetupServer(kClient *kommons.Client) *echo.Echo {
	e := echo.New()
	// Extending the context and fetching the kubeconfig client here.
	// For more info see: https://echo.labstack.com/guide/context/#extending-context
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			cc := &api.Context{
				Kommons: kClient,
				Context: c,
			}
			return next(cc)
		}
	})

	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "apm-hub server running")
	})

	e.POST("/search", pkg.Search)

	return e
}

func init() {
	ServerFlags(Serve.Flags())
}
