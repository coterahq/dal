package cmd

import (
	"github.com/spf13/cobra"
)

type Cli struct {
	rootCmd *cobra.Command
}

func NewCli() *Cli {
	rootCmd := &cobra.Command{
		Use:   "dal",
		Short: "Data Access Layer",
		Long:  `dal is an api for your dbt project.`,
	}
	rootCmd.AddCommand(serveCmd())
	rootCmd.AddCommand(introspectCmd())

	return &Cli{rootCmd: rootCmd}
}

func (c *Cli) Execute() error {
	return c.rootCmd.Execute()
}
