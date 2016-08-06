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

package org.pathirage.freshet.samza;

import org.apache.samza.SamzaException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.StreamJobFactory;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Map;

/**
 * Executes a Samza job given job's configuration.
 */
public class SamzaJobRunner {
  private static Logger log = LoggerFactory.getLogger(SamzaJobRunner.class);

  private static final String SOURCE = "samza-job-runner";

  private static final int WAIT_TIMEOUT = 2000;
  private final MapConfig rawConfig;

  /**
   * Creates an instance of {@link SamzaJobRunner}.
   * @param configurations Samza job configuration
   */
  public SamzaJobRunner(Map<String, String> configurations) {
    rawConfig = new MapConfig(configurations);
  }

  /**
   * Executes Samza job described by {@code rawConfig}.
   * @return Samza StreamJob instance
   */
  public StreamJob run() {
    StreamJobFactory jobFactory;
    JobConfig jobConfig = new JobConfig(rawConfig);

    if(jobConfig.getStreamJobFactoryClass().isEmpty()) {
      throw new SamzaException("No job factory class defined");
    }

    try {
      jobFactory = (StreamJobFactory) Class.forName(jobConfig.getStreamJobFactoryClass().get()).newInstance();
    } catch (Exception e) {
      throw new SamzaException("Cannot instantiate job factory class", e);
    }

    CoordinatorStreamSystemFactory coordinatorStreamSystemFactory = new CoordinatorStreamSystemFactory();
    CoordinatorStreamSystemConsumer coordinatorStreamSystemConsumer =
        coordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(rawConfig, new MetricsRegistryMap());
    CoordinatorStreamSystemProducer coordinatorStreamSystemProducer =
        coordinatorStreamSystemFactory.getCoordinatorStreamSystemProducer(rawConfig, new MetricsRegistryMap());

    log.info("Creating coordinator stream");
    Tuple2<SystemStream, SystemFactory> coordinatorStreamAndFactory =
        Util.getCoordinatorSystemStreamAndFactory(rawConfig);
    SystemAdmin systemAdmin =
        coordinatorStreamAndFactory._2().getAdmin(coordinatorStreamAndFactory._1().getSystem(), rawConfig);
    systemAdmin.createCoordinatorStream(coordinatorStreamAndFactory._1().getStream());

    /*
     * Samza's JobRunner contains logic to create new configuration or modify existing configuration.
     * We don't need that functionality at this stage. We only need to create a new job with new configuration.
     */
    log.info("Storing cofig in coordinator stream");
    coordinatorStreamSystemProducer.register(SOURCE);
    coordinatorStreamSystemProducer.start();
    coordinatorStreamSystemProducer.writeConfig(SOURCE, rawConfig);

    coordinatorStreamSystemConsumer.register();
    coordinatorStreamSystemConsumer.start();
    coordinatorStreamSystemConsumer.bootstrap();
    coordinatorStreamSystemConsumer.stop();

    coordinatorStreamSystemProducer.stop();

    /*
     * Samza's JobRunner also performs some old configuration value clean up and job runner migration.
     * We don't need that at this state.
     */
    // TODO: Deleting old configurations

    // TODO: JobRunnerMigration

    StreamJob job = jobFactory.getJob(jobConfig).submit();

    log.info("Waiting for job to start");

    ApplicationStatus status = job.waitForStatus(ApplicationStatus.Running, WAIT_TIMEOUT);

    if(status == null || !status.equals(ApplicationStatus.Running)) {
      throw new SamzaException("unable to start job successfully. job has status " + status);
    } else {
      log.info("job started successfully - " + status);
    }

    return job;
  }
}
