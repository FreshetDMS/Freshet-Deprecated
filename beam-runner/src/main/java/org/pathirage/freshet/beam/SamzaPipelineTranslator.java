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
import org.apache.beam.sdk.transforms.PTransform;
import org.pathirage.freshet.beam.io.KafkaIO;

import java.util.HashMap;
import java.util.Map;

public class SamzaPipelineTranslator extends Pipeline.PipelineVisitor.Defaults {

  private static final Map<Class<? extends PTransform>, TransformTranslator> transformTranslators = new HashMap();

  static {
    transformTranslators.put(KafkaIO.Read.Unbound.class, new KafkaIOReadUnboundTranslator());
    transformTranslators.put(KafkaIO.Write.Unbound.class, new KafkaIOWriteUnboundTranslator());
  }

  private final SamzaPipelineOptions options;

  public static SamzaPipelineTranslator fromOptions(SamzaPipelineOptions options) {
    return new SamzaPipelineTranslator(options);
  }

  private SamzaPipelineTranslator(SamzaPipelineOptions options) {
    this.options = options;
  }

  public SamzaPipelineJobSpecification translate(Pipeline pipeline) {
    return null;
  }

  private static class KafkaIOReadUnboundTranslator<K,V> implements TransformTranslator<KafkaIO.Read.Unbound<K,V>> {

    @Override
    public void translate(KafkaIO.Read.Unbound<K, V> transform, PipelineTranslationContext translationContext) {

    }
  }

  private static class KafkaIOWriteUnboundTranslator<K,V> implements TransformTranslator<KafkaIO.Write.Unbound<K,V>> {

    @Override
    public void translate(KafkaIO.Write.Unbound<K, V> transform, PipelineTranslationContext translationContext) {

    }
  }
}


