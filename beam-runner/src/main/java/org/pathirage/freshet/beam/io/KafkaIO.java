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

package org.pathirage.freshet.beam.io;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;

import java.util.Map;
import java.util.Set;

import kafka.serializer.Decoder;

/**
 * Read stream from Kafka. This is copied from {@link org.apache.beam.runners.spark.io.KafkaIO}
 */
public final class KafkaIO {

  private KafkaIO() {
  }

  /**
   * Read operation from Kafka topics.
   */
  public static final class Read {

    private Read() {
    }

    /**
     * Define the Kafka consumption.
     *
     * @param keyDecoder   {@link Decoder} to decode the Kafka message key
     * @param valueDecoder {@link Decoder} to decode the Kafka message value
     * @param key          Kafka message key Class
     * @param value        Kafka message value Class
     * @param topics       Kafka topics to subscribe
     * @param kafkaParams  map of Kafka parameters
     * @param <K>          Kafka message key Class type
     * @param <V>          Kafka message value Class type
     * @return KafkaIO Unbound input
     */
    public static <K, V> Unbound<K, V> from(Class<? extends Decoder<K>> keyDecoder,
                                            Class<? extends Decoder<V>> valueDecoder,
                                            Class<K> key,
                                            Class<V> value, Set<String> topics,
                                            Map<String, String> kafkaParams) {
      return new Unbound<>(keyDecoder, valueDecoder, key, value, topics, kafkaParams);
    }

    /**
     * A {@link PTransform} reading from Kafka topics and providing {@link PCollection}.
     */
    public static class Unbound<K, V> extends PTransform<PInput, PCollection<KV<K, V>>> {

      private final Class<? extends Decoder<K>> keyDecoderClass;
      private final Class<? extends Decoder<V>> valueDecoderClass;
      private final Class<K> keyClass;
      private final Class<V> valueClass;
      private final Set<String> topics;
      private final Map<String, String> kafkaParams;

      Unbound(Class<? extends Decoder<K>> keyDecoder,
              Class<? extends Decoder<V>> valueDecoder, Class<K> key,
              Class<V> value, Set<String> topics, Map<String, String> kafkaParams) {
        checkNotNull(keyDecoder, "need to set the key decoder class of a KafkaIO.Read transform");
        checkNotNull(
            valueDecoder, "need to set the value decoder class of a KafkaIO.Read transform");
        checkNotNull(key, "need to set the key class of a KafkaIO.Read transform");
        checkNotNull(value, "need to set the value class of a KafkaIO.Read transform");
        checkNotNull(topics, "need to set the topics of a KafkaIO.Read transform");
        checkNotNull(kafkaParams, "need to set the kafkaParams of a KafkaIO.Read transform");
        this.keyDecoderClass = keyDecoder;
        this.valueDecoderClass = valueDecoder;
        this.keyClass = key;
        this.valueClass = value;
        this.topics = topics;
        this.kafkaParams = kafkaParams;
      }

      public Class<? extends Decoder<K>> getKeyDecoderClass() {
        return keyDecoderClass;
      }

      public Class<? extends Decoder<V>> getValueDecoderClass() {
        return valueDecoderClass;
      }

      public Class<V> getValueClass() {
        return valueClass;
      }

      public Class<K> getKeyClass() {
        return keyClass;
      }

      public Set<String> getTopics() {
        return topics;
      }

      public Map<String, String> getKafkaParams() {
        return kafkaParams;
      }

      @Override
      public PCollection<KV<K, V>> apply(PInput input) {
        // Spark streaming micro batches are bounded by default
        return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
            WindowingStrategy.globalDefault(), PCollection.IsBounded.UNBOUNDED);
      }
    }

  }

  public static final class Write {
    private Write() {
    }

    public static <K,V> Unbound<K,V> from(Class<? extends Coder<K>> keyEncoderClass,
                                          Class<? extends Coder<V>> valueEncoderClass,
                                          Class<K> keyClass, Class<V> valueClass, String topic,
                                          Map<String, String> kafkaParams) {
      return new Unbound<>(keyEncoderClass, valueEncoderClass, keyClass, valueClass, topic, kafkaParams);
    }

    /**
     * A {@link PTransform} reading from Kafka topics and providing {@link PCollection}.
     */
    public static class Unbound<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {
      private final Class<? extends Coder<K>> keyEncoderClass;
      private final Class<? extends Coder<V>> valueEncoderClass;
      private final Class<K> keyClass;
      private final Class<V> valueClass;
      private final String topic;
      private final Map<String, String> kafkaParams;

      Unbound(Class<? extends Coder<K>> keyEncoderClass, Class<? extends Coder<V>> valueEncoderClass,
              Class<K> keyClass, Class<V> valueClass, String topic, Map<String, String> kafkaParams) {
        checkNotNull(keyEncoderClass, "need to set the key encoder class of a KafkaIO.Write transform");
        checkNotNull(
            valueEncoderClass, "need to set the value encoder class of a KafkaIO.Write transform");
        checkNotNull(keyClass, "need to set the key class of a KafkaIO.Write transform");
        checkNotNull(valueClass, "need to set the value class of a KafkaIO.Write transform");
        checkNotNull(topic, "need to set the topics of a KafkaIO.Write transform");
        checkNotNull(kafkaParams, "need to set the kafkaParams of a KafkaIO.Write transform");
        this.keyEncoderClass = keyEncoderClass;
        this.valueEncoderClass = valueEncoderClass;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.topic = topic;
        this.kafkaParams = kafkaParams;
      }

      public Class<? extends Coder<K>> getKeyEncoderClass() {
        return keyEncoderClass;
      }

      public Class<? extends Coder<V>> getValueEncoderClass() {
        return valueEncoderClass;
      }

      public Class<K> getKeyClass() {
        return keyClass;
      }

      public Class<V> getValueClass() {
        return valueClass;
      }

      public String getTopic() {
        return topic;
      }

      public Map<String, String> getKafkaParams() {
        return kafkaParams;
      }

      @Override
      public PDone apply(PCollection<KV<K,V>> input) {
        return PDone.in(input.getPipeline());
      }
    }
  }
}
