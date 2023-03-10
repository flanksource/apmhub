package main

import (
	"fmt"
	"os"

	"github.com/flanksource/flanksource-ui/apm-hub/cmd"
	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "none"    //nolint:staticcheck,unused
	date    = "unknown" //nolint:staticcheck,unused
)

func main() {

	cmd.Root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version of apm-hub",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version)
		},
	})
	cmd.Root.SetUsageTemplate(cmd.Root.UsageTemplate() + fmt.Sprintf("\nversion: %s\n ", version))

	if err := cmd.Root.Execute(); err != nil {
		os.Exit(1)
	}
}
