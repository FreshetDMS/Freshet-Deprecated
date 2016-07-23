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

/**
 * Defines dataflow pipeline that get executed as dag of Samza jobs.
 * {@link SamzaRunner}  translate a {@link org.apache.beam.sdk.Pipeline} instance
 * to a {@link SamzaPipelineJobSpecification} instance that can be executed as a dag of {@link SamzaJobConfig}s locally or in a remote
 * YARN cluster.
 */
public class SamzaPipelineJobSpecification {

  /**
   * First job in a Samza pipeline encapsulating a Beam pipeline.
   */
  private SamzaPipelineNode root;

  public SamzaPipelineJobSpecification(SamzaPipelineNode root) {
    this.root = root;
  }

  public void execute() {
    // Schedule Samza job starting from root
  }
}
