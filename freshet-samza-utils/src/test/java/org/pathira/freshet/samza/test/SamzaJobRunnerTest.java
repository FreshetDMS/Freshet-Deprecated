package org.pathira.freshet.samza.test;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.producer.KeyedMessage;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.local.ThreadJobFactory;
import org.apache.samza.serializers.IntegerSerdeFactory;
import org.apache.samza.system.kafka.KafkaSystemFactory;
import org.junit.Assert;
import org.junit.Test;
import org.pathirage.freshet.samza.SamzaJobConfigBuilder;
import org.pathirage.freshet.samza.SamzaJobRunner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SamzaJobRunnerTest extends AbstractSamzaTest {
  @Test
  public void testSamzaJobRunner() throws Exception {
    SamzaJobConfigBuilder jobConfigBuilder = new SamzaJobConfigBuilder();

    Map<String, String> kafkaConfig = new HashMap<>();
    kafkaConfig.put("consumer.zookeeper.connect", zkServer.connectString());
    kafkaConfig.put("producer.bootstrap.servers", brokers);

    jobConfigBuilder.task(IntDoublerStreamTask.class)
        .addSerde("int", IntegerSerdeFactory.class)
        .addSystem("kafka", KafkaSystemFactory.class, null, null, new MapConfig(kafkaConfig))
        .addStream("kafka", "input", null, "int", false)
        .addInput("kafka", "input")
        .jobCoordinatorSystem("kafka", 1)
        .jobFactory(ThreadJobFactory.class)
        .jobName("test-runner")
        .yarnContainerCount(1)
        .addCustomConfig(IntDoublerStreamTask.OUTPUT_STREAM, "output")
        .addCustomConfig(IntDoublerStreamTask.OUTPUT_SYSTEM, "kafka")
        .addCustomConfig(IntDoublerStreamTask.MAX_MESSAGES, "10");

    createTopic("input", 1);

    final StreamJob job = new SamzaJobRunner(jobConfigBuilder.build()).run();

    List<KeyedMessage> input = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      input.add(createMessage("input", i));
    }

    publish("input", input);

    waitForTopic("output");

    verify("output", "output-consumer-grp-0", new OutputVerifier() {
      @Override
      public void verify(KafkaStream<byte[], byte[]> stream) throws Exception {
        ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
        int i = 0;
        while (consumerIterator.hasNext() && i < 10) {
          i++;
        }

        Assert.assertEquals(10, i);
      }
    });
  }

  private KeyedMessage createMessage(String topic, int n) {
    return new KeyedMessage(topic, ByteBuffer.allocate(4).putInt(n).array());
  }
}
