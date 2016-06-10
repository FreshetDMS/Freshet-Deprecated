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

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.nio.ByteBuffer;

public class IntDoublerStreamTask implements StreamTask, InitableTask {

  public static final String OUTPUT_STREAM = "intdoubler.output.stream";
  public static final String OUTPUT_SYSTEM = "intdoubler.output.system";
  public static final String MAX_MESSAGES = "intdoubler.max.messages";

  private int msgCount = 0;
  private SystemStream outputStream;
  private int maxMessages;

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (msgCount == maxMessages) {
      coordinator.shutdown(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
    }

    collector.send ( new OutgoingMessageEnvelope(outputStream, ByteBuffer.allocate(4).putInt((Integer)envelope.getMessage() * 2).array()));
    msgCount++;
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    outputStream = new SystemStream(config.get(OUTPUT_SYSTEM, "kafka"), config.get(OUTPUT_STREAM, "result"));
    maxMessages = Integer.parseInt(config.get(MAX_MESSAGES, "10"));
  }
}
