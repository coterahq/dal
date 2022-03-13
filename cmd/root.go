package cmd

import (
	"github.com/spf13/cobra"
	"github.com/supasheet/dal/internal/warehouse"
)

type Cli struct {
	rootCmd *cobra.Command
}

func NewCli(w warehouse.Client) *Cli {
	rootCmd := &cobra.Command{
		Use:   "dal",
		Short: "Data Access Layer",
		Long:  `dal is an api for your dbt project.`,
	}
	rootCmd.AddCommand(serveCmd(w))
	rootCmd.AddCommand(introspectCmd(w))

	return &Cli{rootCmd: rootCmd}
}

func (c *Cli) Execute() error {
	return c.rootCmd.Execute()
}
