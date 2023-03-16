package cmd

import (
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

func ServerFlags(flags *pflag.FlagSet) {
	flags.IntVar(&httpPort, "httpPort", 8080, "Port to expose a health dashboard ")
}

func init() {
	logger.BindFlags(Root.PersistentFlags())
	Root.AddCommand(Serve, Operator)
}
