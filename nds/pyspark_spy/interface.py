# -*- coding: utf-8 -*-
#
# SPDX-FileCopyrightText: Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----
#
# Certain portions of the contents of this file are derived from TPC-DS version 3.2.0
# (retrieved from www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
# Such portions are subject to copyrights held by Transaction Processing Performance Council (“TPC”)
# and licensed under the TPC EULA (a copy of which accompanies this file as “TPC EULA” and is also
# available at http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (the “TPC EULA”).
#
# You may not use this file except in compliance with the TPC EULA.
# DISCLAIMER: Portions of this file is derived from the TPC-DS Benchmark and as such any results
# obtained using this file are not comparable to published TPC-DS Benchmark results, as the results
# obtained from using this file do not comply with the TPC-DS Benchmark.
#

# noinspection PyPep8Naming,SpellCheckingInspection
class SparkListenerInterface(object):
    """
    SparkListener python interface.

    https://spark.apache.org/docs/3.2.1/api/java/org/apache/spark/scheduler/SparkListener.html
    """

    def onApplicationEnd(self, applicationEnd):
        pass
    def onApplicationStart(self, applicationStart):
        pass
    def onBlockManagerAdded(self, blockManagerAdded):
        pass
    def onBlockManagerRemoved(self, blockManagerRemoved):
        pass
    def onBlockUpdated(self, blockUpdated):
        pass
    def onEnvironmentUpdate(self, environmentUpdate):
        pass
    def onExecutorAdded(self, executorAdded):
        pass
    def onExecutorBlacklisted(self, executorBlacklisted):
        pass
    def onExecutorBlacklistedForStage(self, executorBlacklistedForStage):
        pass
    def onExecutorExcluded(self, executorExcluded):
        pass
    def onExecutorExcludedForStage(self, executorExcludedForStage):
        pass
    def onExecutorMetricsUpdate(self, executorMetricsUpdate):
        pass
    def onExecutorRemoved(self, executorRemoved):
        pass
    def onExecutorUnblacklisted(self, executorUnblacklisted):
        pass
    def onExecutorUnexcluded(self, executorUnexcluded):
        pass
    def onJobEnd(self, jobEnd):
        pass
    def onJobStart(self, jobStart):
        pass
    def onNodeBlacklisted(self, nodeBlacklisted):
        pass
    def onNodeBlacklistedForStage(self, nodeBlacklistedForStage):
        pass
    def onNodeExcluded(self, nodeExcluded):
        pass
    def onNodeExcludedForStage(self, nodeExcludedForStage):
        pass
    def onNodeUnblacklisted(self, nodeUnblacklisted):
        pass
    def onNodeUnexcluded(self, nodeUnexcluded):
        pass
    def onOtherEvent(self, event):
        pass
    def onResourceProfileAdded(self, resourceProfileAdded):
        pass
    def onSpeculativeTaskSubmitted(self, speculativeTaskSubmitted):
        pass
    def onStageCompleted(self, stageCompleted):
        pass
    def onStageExecutorMetrics(self, stageExecutorMetrics):
        pass
    def onStageSubmitted(self, stageSubmitted):
        pass
    def onTaskEnd(self, taskEnd):
        pass
    def onTaskGettingResult(self, taskGettingResult):
        pass
    def onTaskStart(self, taskStart):
        pass
    def onUnpersistRDD(self, unpersistRDD):
        pass
    def onUnschedulableTaskSetAdded(self, unschedulableTaskSetAdded):
        pass
    def onUnschedulableTaskSetRemoved(self, unschedulableTaskSetRemoved):
        pass

    class Java:
        """A necessary class to implement Java interface in Py4j
        """
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]
