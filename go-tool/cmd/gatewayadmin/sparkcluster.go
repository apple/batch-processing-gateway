/*
 *
 * This source file is part of the Batch Processing Gateway open source project
 *
 * Copyright 2022 Apple Inc. and the Batch Processing Gateway project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"github.com/spf13/cobra"
)

var clusterCmd = &cobra.Command{
	Use:   "spark-cluster",
	Short: "Manage Spark cluster configuration and credential",
	Long:  `Manage Spark cluster configuration and credential`,
}

func init() {
	clusterCmd.AddCommand(addSparkClusterCmd)
}

var addSparkClusterCmd = &cobra.Command{
	Use:   "add",
	Short: "add a Spark Cluster",
	Long:  `Add a Spark Cluster.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			exitWithError("Please specify an argument to identify kubeconfig context for the Spark Cluster")
		}
	},
}
