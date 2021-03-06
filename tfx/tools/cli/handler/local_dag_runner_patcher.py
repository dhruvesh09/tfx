# Copyright 2021 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Patches LocalDagRunner to bypass run() function execution."""

from typing import Type

from tfx.orchestration.local import local_dag_runner
from tfx.orchestration.portable import tfx_runner
from tfx.tools.cli.handler import dag_runner_patcher


class LocalDagRunnerPatcher(dag_runner_patcher.DagRunnerPatcher):
  """Patches LocalDagRunner.run() for CLI."""

  def __init__(self):
    super().__init__(call_real_run=False)

  def get_runner_class(self) -> Type[tfx_runner.TfxRunner]:
    return local_dag_runner.LocalDagRunner
