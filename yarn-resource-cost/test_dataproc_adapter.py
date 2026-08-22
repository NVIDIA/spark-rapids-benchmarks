#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import subprocess
import unittest
from unittest import mock

import yarn_job_cost_dataproc as dataproc


class DataprocAdapterTest(unittest.TestCase):
    @mock.patch.object(dataproc, "run_command")
    def test_primary_and_secondary_worker_shapes_remain_distinct(self, run):
        run.return_value = subprocess.CompletedProcess(
            ["gcloud"],
            0,
            stdout=json.dumps(
                {
                    "clusterUuid": "fixture-uuid",
                    "config": {
                        "workerConfig": {
                            "machineTypeUri": "zones/us-west1-a/machineTypes/n2-standard-16",
                            "instanceNames": ["sample-w-0"],
                        },
                        "secondaryWorkerConfig": {
                            "machineTypeUri": "zones/us-west1-a/machineTypes/g2-standard-16",
                            "instanceNames": ["sample-sw-0"],
                            "accelerators": [
                                {
                                    "acceleratorTypeUri": (
                                        "zones/us-west1-a/acceleratorTypes/nvidia-l4"
                                    ),
                                    "acceleratorCount": 1,
                                }
                            ],
                        },
                    },
                }
            ),
            stderr="",
        )
        mappings, provenance = dataproc.describe_node_classes(
            "sample", "us-west1", "example-project"
        )
        self.assertEqual("gcp:dataproc:n2-standard-16", mappings["sample-w-0"])
        self.assertEqual(
            "gcp:dataproc:g2-standard-16+1xnvidia-l4",
            mappings["sample-sw-0"],
        )
        self.assertEqual("fixture-uuid", provenance["cluster_uuid"])


if __name__ == "__main__":
    unittest.main()
