/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.server.commit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorListener;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

/**
 * Test TwoPhaseCommit behaves as expected
 */
@Category(SmallTests.class)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestTwoPhaseCommit {

  private ExceptionSnare<Exception> monitor = new ExceptionSnare<Exception>();
  private DistributedThreePhaseCommitErrorListener listener = Mockito.mock(DistributedThreePhaseCommitErrorListener.class);
  private final long wakeFrequency = 50;

  @After
  public void resetMocks() {
    Mockito.reset(listener);
  }

  private static final VerificationMode once = times(1);

  @Test(timeout = 500)
  public void testSingleLatchCount() throws Exception {
    ThreePhaseCommit op = Mockito.spy(new CheckableTwoPhaseCommit(monitor, listener, wakeFrequency));
    // start the two phase commit
    new Thread(op).start();

    // wait for the commit phase
    op.getPreparedLatch().await();
    verify(op, once).prepare();

    // count down the commit phase
    op.getAllowCommitLatch().countDown();
    op.getCompletedLatch().await();
    verify(op, once).commit();
    verify(op, once).finish();
    verify(op, never()).cleanup(Mockito.any(Exception.class));
    Mockito.verifyZeroInteractions(listener);
  }

  @Test(timeout = 500)
  public void testMultipleLatchCounts() throws Exception {
    // now do a test with multiple counts for each latch
    final ThreePhaseCommit op = Mockito.spy(new CheckableTwoPhaseCommit(monitor, listener,
        wakeFrequency, 2, 2, 2, 2));
    // count down the prepared latch in the operation
    Mockito.doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        op.getPreparedLatch().countDown();
        return null;
      }
    }).when(op).prepare();
    // start the two phase commit
    new Thread(op).start();

    // wait for the prepare phase to finish
    // this is fast, so shouldn't be too bad
    op.getPreparedLatch().await();
    assertEquals("Prepared latch didn't count down correctly", 0, op.getPreparedLatch().getCount());
    verify(op, once).prepare();

    // count down the commit phase
    op.getAllowCommitLatch().countDown();
    // shouldn't have committed yet
    verify(op, never()).commit();

    // preempt the finish & complete latch to we can wait on that
    op.getCommitFinishedLatch().countDown();
    op.getCompletedLatch().countDown();
    assertEquals("Finish latch counted down prematurely", 1, op.getCommitFinishedLatch().getCount());
    assertEquals("Completed latch counted down prematurely", 1, op.getCompletedLatch().getCount());

    // this should cause commit
    op.getAllowCommitLatch().countDown();
    assertTrue("Finished latch didn't count down correclty",
      op.getCommitFinishedLatch().getCount() >= 1);
    // wait for the operation to finish
    op.getCommitFinishedLatch().await();

    // wait for the finish phase to run
    op.getCompletedLatch().await();
    verify(op, once).commit();
    verify(op, once).finish();
    Mockito.verifyZeroInteractions(listener);
  }

  @Test(timeout = 1000)
  public void testErrorPropagation() throws Exception {
    // use own own monitor here to not munge the rest of the test
    ExceptionSnare<Exception> monitor = new ExceptionSnare<Exception>();
    ThreePhaseCommit tpc = Mockito.spy(new CheckableTwoPhaseCommit(monitor, listener, wakeFrequency));
    DistributedCommitException cause = new DistributedCommitException("Example DCE");
    monitor.receiveError("test before commit starts", cause);
    Thread t = new Thread(tpc);
    t.start();
    t.join();
    verify(tpc, never()).prepare();
    verify(tpc, never()).commit();
    verify(tpc, once).cleanup(cause);
    verify(tpc, once).finish();
    verify(listener, once).localOperationException(CommitPhase.PRE_PREPARE, cause);
    Mockito.reset(listener);

    // now test that we can put an error in before the commit phase runs
    monitor = new ExceptionSnare<Exception>();
    tpc = Mockito.spy(new CheckableTwoPhaseCommit(monitor, listener, wakeFrequency));
    t = new Thread(tpc);
    t.start();
    tpc.getPreparedLatch().await();
    monitor.receiveError("test after prepare", cause);
    t.join();

    // verify state of all the object
    verify(tpc, once).prepare();
    verify(tpc, once).cleanup(cause);
    verify(tpc, once).finish();
    verify(tpc, never()).commit();
    verify(listener, once).localOperationException(CommitPhase.PRE_COMMIT, cause);
  }
}