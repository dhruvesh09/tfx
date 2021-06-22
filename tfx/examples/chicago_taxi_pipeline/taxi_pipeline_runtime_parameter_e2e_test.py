# Copyright 2019 Google LLC. All Rights Reserved.
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
"""End-to-end tests for tfx.examples.chicago_taxi_pipeline.taxi_pipeline_runtime_parameter."""

import os
import subprocess
from absl import logging

import tensorflow as tf

from tfx.examples.chicago_taxi_pipeline import taxi_pipeline_runtime_parameter
from tfx.orchestration import test_utils
from tfx.orchestration.kubeflow import test_utils as kubeflow_test_utils


class TaxiPipelineRuntimeParameterEndToEndTest(
    kubeflow_test_utils.BaseKubeflowTest):

  def testEndToEndPipelineRun(self):
    """End-to-end test for pipeline with RuntimeParameter."""
    pipeline_name = 'kubeflow-e2e-test-parameter-{}'.format(
        test_utils.random_id())
    pipeline_root = self._pipeline_root(pipeline_name)

    transform_module_path = os.path.join(
        pipeline_root, os.path.basename(self._transform_module))
    trainer_module_path = os.path.join(pipeline_root,
                                       os.path.basename(self._trainer_module))
    # Upload module files to be available for the components.
    # TODO(b/174289068): Move to dedicated GCS utils.
    subprocess.run(
        ['gsutil', 'cp', self._transform_module, transform_module_path],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.run(
        ['gsutil', 'cp', self._trainer_module, trainer_module_path],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    pipeline = taxi_pipeline_runtime_parameter._create_parameterized_pipeline(
        pipeline_name=pipeline_name,
        pipeline_root=pipeline_root,
        transform_module_file=transform_module_path,
        trainer_module_file=trainer_module_path,
        enable_cache=True,
        beam_pipeline_args=taxi_pipeline_runtime_parameter._beam_pipeline_args)

    parameters = {
        'data-root': self._data_root,
        'train-steps': 10,
        'eval-steps': 5,
        'slicing-column': 'trip_start_hour',
    }

    self._compile_and_run_pipeline(pipeline=pipeline, parameters=parameters)


if __name__ == '__main__':
  logging.set_verbosity(logging.INFO)
  tf.test.main()
