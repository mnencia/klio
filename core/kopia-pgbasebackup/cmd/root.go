/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// rootCmd is the kopia-pgbasebackup root command.
//
//nolint:gochecknoglobals
var rootCmd = &cobra.Command{
	Use:   "kopia-pgbasebackup",
	Short: "Ingest a PostgreSQL BASE_BACKUP tar stream into a Kopia repository",
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		// Silence usage for runtime errors. Usage is still shown for flag
		// parsing and args validation errors because those occur before
		// PersistentPreRunE is called.
		cmd.SilenceUsage = true

		return nil
	},
}

// Execute runs the root command. It is called once from main.main.
func Execute(ctx context.Context) {
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

//nolint:gochecknoinits
func init() {
	// KOPIA_PASSWORD is the repository encryption password, set by the
	// caller (core/internal/kopia.PgBaseBackupClient) as a process env var
	// rather than a flag, matching how the kopia CLI itself takes it.
	_ = viper.BindEnv("kopia-password", "KOPIA_PASSWORD")

	rootCmd.AddCommand(ingestCmd)
}
