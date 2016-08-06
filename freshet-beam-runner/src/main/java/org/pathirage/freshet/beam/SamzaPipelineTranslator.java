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
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PValue;
import org.pathirage.freshet.beam.io.KafkaIO;
import org.pathirage.freshet.samza.SamzaJobConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SamzaPipelineTranslator extends Pipeline.PipelineVisitor.Defaults implements PipelineTranslationContext {
  private static Logger log = LoggerFactory.getLogger(SamzaPipelineTranslator.class);

  private static final Map<Class<? extends PTransform>, TransformTranslator> transformTranslators = new HashMap();

  static {
    transformTranslators.put(KafkaIO.Read.Unbound.class, new KafkaIOReadUnboundTranslator());
    transformTranslators.put(KafkaIO.Write.Unbound.class, new KafkaIOWriteUnboundTranslator());
    transformTranslators.put(ParDo.Bound.class, new ParDoBoundTranslator());
  }

  private final SamzaPipelineOptions options;

  private final SamzaPipelineSpecification samzaPipelineSpec = new SamzaPipelineSpecification();

  private final SamzaJobConfigBuilder samzaJobConfigBuilder = new SamzaJobConfigBuilder();

  public static SamzaPipelineTranslator fromOptions(SamzaPipelineOptions options) {
    return new SamzaPipelineTranslator(options);
  }

  private static String formatNodeName(TransformTreeNode node) {
    return node.toString().split("@")[1] + node.getTransform();
  }

  private static TransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
    return transformTranslators.get(transform.getClass());
  }

  private SamzaPipelineTranslator(SamzaPipelineOptions options) {
    this.options = options;
  }

  public SamzaPipelineSpecification translate(Pipeline pipeline) {
    pipeline.traverseTopologically(this);
    return samzaPipelineSpec;
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
    return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformTreeNode node) {

  }

  @Override
  public void visitPrimitiveTransform(TransformTreeNode node) {
    log.info("visitPrimitiveTransform-" + formatNodeName(node));
    PTransform<?, ?> transform = node.getTransform();
    TransformTranslator translator = getTranslator(transform);

    if (translator == null) {
      log.error("Could not found translator for " + transform.getClass());
      throw new UnsupportedOperationException("The transform " + transform.getClass() + " is not supported yet.");
    }

    translator.translate(transform, this);
  }

  @Override
  public void visitValue(PValue value, TransformTreeNode producer) {
    throw new UnsupportedOperationException("Values are not supported yet.");
  }

  private static class KafkaIOReadUnboundTranslator<K, V> implements TransformTranslator<KafkaIO.Read.Unbound<K, V>> {

    @Override
    public void translate(KafkaIO.Read.Unbound<K, V> transform, PipelineTranslationContext translationContext) {

    }
  }

  private static class KafkaIOWriteUnboundTranslator<K, V> implements TransformTranslator<KafkaIO.Write.Unbound<K, V>> {

    @Override
    public void translate(KafkaIO.Write.Unbound<K, V> transform, PipelineTranslationContext translationContext) {

    }
  }

  private static class ParDoBoundTranslator<InputT, OutputT> implements TransformTranslator<ParDo.Bound<InputT, OutputT>> {

    @Override
    public void translate(ParDo.Bound<InputT, OutputT> transform, PipelineTranslationContext translationContext) {

    }
  }
}


