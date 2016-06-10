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

package org.pathira.freshet.samza.test;

import com.google.common.io.Resources;
import com.google.gson.Gson;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.SamzaException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Defines setup and teardown methods require for tests involving Samza jobs. Setup method provision
 * necessary services (Embedded Kafka and Zookeeper) for a local Samza job. Also provide some helper
 * methods to create Kafka topics, publish messages and consume messages to/from a Kafka topic.
 */
public abstract class AbstractSamzaTest {
  static ZkClient zkClient;
  static EmbeddedZookeeper zkServer;
  static int brokerPort = TestUtils.choosePort();
  static String brokers = String.format("localhost:%s", brokerPort);
  static KafkaServer kafkaServer;
  static List<KafkaServer> kafkaServers;

  @BeforeClass
  public static void setup() {
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    kafkaServer = TestUtils.createServer(new KafkaConfig(TestUtils.createBrokerConfig(0, brokerPort, true)),
        new MockTime());
    kafkaServers = new ArrayList<>();
    kafkaServers.add(kafkaServer);
  }

  @AfterClass
  public static void teardown() {
    kafkaServer.shutdown();
    zkServer.shutdown();
  }

  protected void createTopic(String topic, int partitions) {
    String[] args = new String[]{"--topic", topic, "--partitions", String.valueOf(partitions), "--replication-factor",
        "1"};
    TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(args));
    waitForTopic(topic);
  }

  protected void waitForTopic(String topic) {
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(kafkaServers), topic, 0,
        30000);
  }

  protected void deleteTopic(String topic) {
    TopicCommand.deleteTopic(zkClient, new TopicCommand.TopicCommandOptions(new String[]{"--topic", topic}));
  }

  protected void publish(String topic, List<KeyedMessage> messages) {
    // TODO: Explore how to allow custom partitioning
    Producer producer = new Producer(new ProducerConfig(TestUtils.getProducerConfig(brokers)));
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    producer.close();
  }

  protected List<Map<Object, Object>> readMessages(String path, String type) throws IOException {
    Gson gson = new Gson();
    Map<Object, Object> testData = (Map<Object, Object>) gson.fromJson(
        Resources.toString(AbstractSamzaTest.class.getResource(path), Charset.defaultCharset()),
        Map.class);
    if (testData.containsKey(type)) {
      return (List<Map<Object, Object>>) testData.get(type);
    } else {
      throw new SamzaException("Invalid test data format. Cannot find input messages.");
    }
  }

  protected String resourceToString(String path) throws IOException {
    return Resources.toString(AbstractSamzaTest.class.getResource(path), Charset.defaultCharset());
  }

  protected void deleteConsumerGroup(String group) {
    zkClient.delete(String.format("/consumers/%s", group));
  }

  protected void verify(String topic, String consumerGroup, OutputVerifier verifier) throws Exception {
    Properties consumerProperties = TestUtils.createConsumerProperties(zkServer.connectString(), consumerGroup, "consumer0", -1);
    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);

    verifier.verify(stream);
  }

  public static interface OutputVerifier {
    void verify(KafkaStream<byte[], byte[]> stream) throws Exception;
  }

}
