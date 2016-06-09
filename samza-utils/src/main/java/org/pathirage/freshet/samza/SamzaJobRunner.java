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
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.StreamJobFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SamzaJobRunner implements Runnable {
  private static Logger log = LoggerFactory.getLogger(SamzaJobRunner.class);

  private static final int WAIT_TIMEOUT = 2000;
  private final MapConfig rawConfig;

  public SamzaJobRunner(Map<String, String> configurations) {
    rawConfig = new MapConfig(configurations);
  }

  @Override
  public void run() {
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

    StreamJob job = jobFactory.getJob(rawConfig).submit();

    log.info("Waiting for job to start");

    ApplicationStatus status = job.waitForStatus(ApplicationStatus.Running, WAIT_TIMEOUT);

    if(status == null) {
      throw new SamzaException("unable to start job successfully.");
    } else if (status.equals(ApplicationStatus.Running)) {
      log.info("job started successfully - " + status);
    } else {
      log.warn("unable to start job successfully. job has status " + status);
    }
  }
}
