/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.server.commit;

import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedErrorListener;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.mockito.Mockito;

/**
 * An empty two-phase commit operation. Should be used with {@link Mockito#spy(Object)} to check for
 * progress.
 */
public class CheckableTwoPhaseCommit extends
    ThreePhaseCommit {
  public boolean prepared = false;
  boolean commit = false;
  boolean cleanup = false;
  boolean finish = false;

  public CheckableTwoPhaseCommit(ExceptionSnare<DistributedCommitException> monitor,
      DistributedErrorListener errorListener, long wakeFrequency) {
    super(monitor, errorListener, wakeFrequency);
  }
//
//  public CheckableTwoPhaseCommit(ExceptionSnare<DistributedCommitException> monitor,
//      DistributedErrorListener errorListener, long wakeFrequency,
//      int i,
//      int j, int k, int l) {
//    // super long timeout
//    super(monitor, errorListener, wakeFrequency, i, j, k, l, Integer.MAX_VALUE);
//  }

  @Override
  public void prepare() throws DistributedCommitException {
  }

  @Override
  public void commit() throws DistributedCommitException {
  }

  @Override
  public void cleanup(Exception e) {
  }

  @Override
  public void finish() {
  }

  @Override
  public void prepared(String node) {
  }

  @Override
  public void committed(String node) {
  }
}