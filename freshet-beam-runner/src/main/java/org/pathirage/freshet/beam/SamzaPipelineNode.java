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

import java.util.ArrayList;
import java.util.List;

/**
 * Defines a node in a Samza job pipeline generated from a dataflow pipeline.
 */
public class SamzaPipelineNode {
  private final List<SamzaPipelineNode> successors = new ArrayList<>();

  /**
   * Actual Samza jobConfig
   */
  private final SamzaJobConfig jobConfig;

  public SamzaPipelineNode(SamzaJobConfig jobConfig) {
    this.jobConfig = jobConfig;
  }

  public void addSuccessor(SamzaPipelineNode successor) {
    successors.add(successor);
  }

  public List<SamzaPipelineNode> getSuccessors() {
    return successors;
  }
}
