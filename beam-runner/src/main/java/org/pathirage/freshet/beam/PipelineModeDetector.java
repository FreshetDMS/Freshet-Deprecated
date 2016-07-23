/**
 * Copyright 2016 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pathirage.freshet.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;

public class PipelineModeDetector extends Pipeline.PipelineVisitor.Defaults {

  private final SamzaPipelineOptions options;

  boolean streaming = false;

  public PipelineModeDetector(SamzaPipelineOptions options) {
    this.options = options;
  }

  public boolean isStreaming(Pipeline pipeline) {
    if(options.isStreaming()) {
      return true;
    }

    pipeline.traverseTopologically(this);

    return streaming;
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformTreeNode node) {
  }

  @Override
  public void visitPrimitiveTransform(TransformTreeNode node) {
    Class<? extends PTransform> transformClass = node.getTransform().getClass();
    if (transformClass == Read.Unbounded.class) {
      streaming = true;
    }
  }

  @Override
  public void visitValue(PValue value, TransformTreeNode producer) {
  }
}
