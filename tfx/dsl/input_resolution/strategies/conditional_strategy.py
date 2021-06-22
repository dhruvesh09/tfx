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
"""Experimental Resolver for evaluating the condition."""

from typing import Dict, List, Optional, Text

from tfx import types
from tfx.dsl.compiler import placeholder_utils
from tfx.dsl.components.common import resolver
from tfx.orchestration import data_types
from tfx.orchestration import metadata
from tfx.orchestration.portable import data_types as portable_data_types
from tfx.proto.orchestration import placeholder_pb2


class ConditionalStrategy(resolver.ResolverStrategy):
  """Strategy that resolves artifacts if predicates are met."""

  def __init__(self, predicates: List[placeholder_pb2.PlaceholderExpression]):
    self._predicates = predicates

  def resolve(
      self,
      pipeline_info: data_types.PipelineInfo,
      metadata_handler: metadata.Metadata,
      source_channels: Dict[Text, types.Channel],
  ) -> resolver.ResolveResult:
    raise NotImplementedError()

  def resolve_artifacts(
      self, metadata_handler: metadata.Metadata,
      input_dict: Dict[Text, List[types.Artifact]]
  ) -> Optional[Dict[Text, List[types.Artifact]]]:
    for placeholder_pb in self._predicates:
      context = placeholder_utils.ResolutionContext(
          exec_info=portable_data_types.ExecutionInfo(input_dict=input_dict))
      predicate_result = placeholder_utils.resolve_placeholder_expression(
          placeholder_pb, context)
      if not isinstance(predicate_result, bool):
        raise ValueError("Predicate evaluates to a non-boolean result.")
      if not predicate_result:
        return None
    return input_dict
