package cmd

import (
	"net/http"
	"strconv"

	"github.com/flanksource/apm-hub/api"
	"github.com/flanksource/apm-hub/api/logs"
	"github.com/flanksource/apm-hub/db"
	"github.com/flanksource/apm-hub/pkg"
	"github.com/flanksource/commons/logger"
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

	db.DeleteOldConfigFileBackends()
	if len(configFiles) != 0 {
		for _, configFile := range configFiles {
			logger.Debugf("parsing config file: %s", configFile)
			config, err := pkg.ParseConfig(configFile)
			if err != nil {
				logger.Errorf("error parsing the configFile: %v", err)
				continue
			}

			err = db.PersistLoggingBackendConfigFile(*config)
			if err != nil {
				logger.Errorf("error persisting backend to file: %v", err)
				continue
			}
		}
	}
	err = pkg.LoadGlobalBackends()
	if err != nil {
		logger.Fatalf("error loading backends: %v", err)
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
