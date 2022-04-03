package main

import (
	"os"

	"github.com/supasheet/dal/cmd"
)

func main() {
	cli := cmd.NewCli()
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
