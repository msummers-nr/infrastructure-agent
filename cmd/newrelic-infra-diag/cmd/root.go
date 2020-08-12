package cmd

import (
	"github.com/newrelic/infrastructure-agent/pkg/log"
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "cmd-example",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
}

var clog = log.WithComponent("cmd")

var Verbose bool
var Config string

func init() {
	rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "verbose output")
	rootCmd.PersistentFlags().StringVarP(&Config, "config", "c", "", "Override default configuration file")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		clog.WithError(err).Error("command did not execute successfully")
		os.Exit(1)
	}
}
