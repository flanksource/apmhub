package cmd

import (
	"os"

	"github.com/flanksource/apm-hub/db"
	"github.com/flanksource/commons/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var Root = &cobra.Command{
	Use: "apm-hub",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		logger.UseZap(cmd.Flags())
	},
}

var httpPort int
var metricsPort int

func ServerFlags(flags *pflag.FlagSet) {
	flags.IntVar(&httpPort, "httpPort", 8080, "Port to expose the http server")
	flags.IntVar(&metricsPort, "metricsPort", 8081, "Port to expose a health dashboard")
}

func readFromEnv(v string) string {
	val := os.Getenv(v)
	if val != "" {
		return val
	}
	return v
}

func init() {
	logger.BindFlags(Root.PersistentFlags())
	db.Flags(Root.PersistentFlags())

	db.ConnectionString = readFromEnv(db.ConnectionString)
	if err := db.Init(db.ConnectionString); err != nil {
		logger.Fatalf("Failed to initialize the db: %v", err)
	}

	Root.AddCommand(Serve, Operator)
}
