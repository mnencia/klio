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

// Command kopia-pgbasebackup ingests a PostgreSQL BASE_BACKUP tar stream
// (delivered on stdin) into a Kopia repository, building a self-contained
// snapshot bottom-up. It exists as a separate binary so the Kopia Go library
// never enters the main klio binary; klio invokes it via exec, exactly as it
// invokes the kopia CLI.
//
// The helper code lives in this repository. It consumes the Kopia library under
// its canonical import path; a replace directive in go.mod currently sources it
// from the Klio fork until the patches are merged upstream.
package main

import (
	"context"

	"github.com/cloudnative-pg/klio/core/kopia-pgbasebackup/cmd"
)

func main() {
	cmd.Execute(context.Background())
}
