package cmd

import (
	"errors"
	"github.com/newrelic/infrastructure-agent/pkg/config"
	config2 "github.com/newrelic/infrastructure-agent/pkg/integrations/v4/config"
	"github.com/newrelic/infrastructure-agent/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path/filepath"
)

// validateCmd represents the validate command
var validateCmd = &cobra.Command{
	Use:   "validate [config location (optional)]",
	Short: "Validate infra agent configurations",
	Long: `Validate infra agent configurations that exist
and usage of using your command. For example:
newrelic-infra-diag validate integration-config.yml
If no config location added it will load the infra
agents configurations from the config.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if Verbose {
			log.SetLevel(logrus.DebugLevel)
		}

		acfg, err := config.LoadConfig(Config)
		checkError(err, "got an error loading agent config", 1)
		clog.Infof("Found plugin dir: %s", acfg.PluginDir)
		err = validatePluginDir(acfg.PluginDir)
		checkError(err, "failed validating plugin dir", 2)
	},
}

func checkError(err error, message string, exitCode int) {
	if err != nil {
		clog.WithError(err).Error(message)
		os.Exit(exitCode)
	}
}

func init() {
	rootCmd.AddCommand(validateCmd)

	validateCmd.PersistentFlags().String("config", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// validateCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func validatePluginDir(pluginDir string) error {
	var files []string
	err := filepath.Walk(pluginDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return err
	}

	var errs []error
	for i := range files {
		cfg := files[i]
		cfgLog := clog.WithField("File", cfg)
		bytes, err := ioutil.ReadFile(cfg)
		if err != nil {
			cfgLog.WithError(err).Error("Failed to read file")
			errs = append(errs, err)
		}
		m, err := parseYAML(cfgLog, bytes)
		if err != nil {
			cfgLog.WithError(err).Error("Invalid yaml")
			errs = append(errs, err)
			continue
		}
		clog.Debugf("Got %v", m)
		cy, err := parseConfigYAML(cfgLog, bytes)
		if err != nil {
			cfgLog.WithError(err).Error("None valid configuration")
			errs = append(errs, err)
			continue
		}
		clog.WithField("cy", cy).WithField("bs", string(bytes)).Infof("All good")
	}

	if len(errs) > 0 {
		return errors.New("one or more config files were invalid")
	}
	return nil
}

func parseYAML(cfgLog log.Entry, bytes []byte) (map[string]interface{}, error) {
	cfgLog.Info("Validating yaml...")
	var cy map[string]interface{}
	if err := yaml.UnmarshalStrict(bytes, &cy); err != nil {
		return nil, err
	}
	return cy, nil
}

func parseConfigYAML(cfgLog log.Entry, bytes []byte) (config2.YAML, error) {
	cfgLog.Info("Validating configuration...")
	var cy config2.YAML
	if err := yaml.UnmarshalStrict(bytes, &cy); err != nil {
		return config2.YAML{}, err
	}
	return cy, nil
}
