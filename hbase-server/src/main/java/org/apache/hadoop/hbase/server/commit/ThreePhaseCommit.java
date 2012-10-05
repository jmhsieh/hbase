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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedErrorListener;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedThreePhaseCommitErrorDispatcher;
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitCoordinatorController;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.DistributedThreePhaseCommitCoordinator;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.collect.Lists;

/**
 * A two-phase commit that enforces a time-limit on the operation. If the time limit expires before
 * the operation completes, the {@link DistributedErrorListener} will receive a
 * {@link OperationAttemptTimeoutException} from the {@link OperationAttemptTimer}.
 * <p>
 * This is particularly useful for situations when running a distributed {@link TwoPhaseCommit} so
 * participants can avoid blocking for extreme amounts of time if one of the participants fails or
 * takes a really long time (e.g. GC pause).
 * @param <T> stored error listener to watch for errors from the operation
 * @param <E> type of exception the monitor will throw
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ThreePhaseCommit implements Callable<Void>, Runnable { 
  /** latch counted down when the prepared phase completes */
  private final CountDownLatch preparedLatch;
  /** waited on before allowing the commit phase to proceed */
  private final CountDownLatch allowCommitLatch;
  /** counted down when the commit phase completes */
  private final CountDownLatch commitFinishLatch;
  /** counted down when the {@link #finish()} phase completes */
  private final CountDownLatch completedLatch;
  /** monitor to check for errors */
  private final ExceptionCheckable<DistributedCommitException> errorMonitor;
  /** listener to listen for any errors to the operation */
  private final DistributedErrorListener errorListener;
  /** The current phase the operation is in */
  protected CommitPhase phase = CommitPhase.PRE_PREPARE;
  /** frequency to check for errors (ms) */
  protected final long wakeFrequency;
  
  private static final Log LOG = LogFactory.getLog(ThreePhaseCommit.class);
  protected final OperationAttemptTimer timer;

  /** lock to prevent nodes from preparing and then committing before we can track them */
  private Object joinBarrierLock = new Object();
  private final DistributedCommitCoordinatorController controller;
  private final List<String> prepareNodes;
  private final List<String> commitingNodes;
  private CountDownLatch allServersPreparedOperation;
  private CountDownLatch allServersCompletedOperation;
  private String opName;
  private byte[] data;

  private DistributedThreePhaseCommitCoordinator parent;
  
  /**
   * {@link ThreePhaseCommit} operation run by a {@link DistributedThreePhaseCommitCoordinator} for
   * a given operation
   * @param parent parent coordinator to call back to for general errors (e.g.
   *          {@link DistributedThreePhaseCommitCoordinator#controllerConnectionFailure(String, IOException)}
   *          ).
   * @param controller coordinator controller to update with progress
   * @param monitor error monitor to check for errors to the operation
   * @param wakeFreq frequency to check for errors while waiting
   * @param timeout amount of time to allow the operation to run
   * @param timeoutInfo information to pass along to the error monitor if there is a timeout
   * @param opName name of the operation to run
   * @param data information associated with the operation
   * @param expectedPrepared names of the expected cohort members
   */
  public ThreePhaseCommit(DistributedThreePhaseCommitCoordinator parent,
      DistributedCommitCoordinatorController controller,
      DistributedThreePhaseCommitErrorDispatcher monitor, long wakeFreq, long timeout,
      Object[] timeoutInfo, String opName, byte[] data, List<String> expectedPrepared) {
    this.errorMonitor = monitor;
    this.errorListener = monitor;
    this.wakeFrequency = wakeFreq;
    this.preparedLatch = new CountDownLatch(1);
    this.allowCommitLatch = new CountDownLatch(expectedPrepared.size());
    this.commitFinishLatch = new CountDownLatch(expectedPrepared.size());
    this.completedLatch = new CountDownLatch(1);
    this.timer = setupTimer(errorListener, timeout);

    this.parent = parent;
    this.controller = controller;
    this.prepareNodes = new ArrayList<String>(expectedPrepared);
    this.commitingNodes = new ArrayList<String>(prepareNodes.size());

    this.opName = opName;
    this.data = data;

    // block the commit phase until all the servers prepare
    this.allServersPreparedOperation = this.getAllowCommitLatch();
    // allow interested watchers to block until we are done with the commit phase
    this.allServersCompletedOperation = this.getCommitFinishedLatch();
  }

  /**
   * Testing only
   * @param monitor
   * @param errorListener
   * @param wakeFrequency
   */
  public ThreePhaseCommit(ExceptionCheckable<DistributedCommitException> monitor, DistributedErrorListener errorListener,
      long wakeFrequency) {
    // Default to a very large timeout
    this.errorMonitor = monitor;
    this.errorListener = errorListener;
    this.wakeFrequency = wakeFrequency;
    this.preparedLatch = new CountDownLatch(1);
    this.allowCommitLatch = new CountDownLatch(1);
    this.commitFinishLatch = new CountDownLatch(1);
    this.completedLatch = new CountDownLatch(1);
    this.timer = setupTimer(errorListener, Integer.MAX_VALUE);

    this.parent = null;
    this.controller = null;
    this.prepareNodes = new ArrayList<String>();
    this.commitingNodes = new ArrayList<String>(prepareNodes.size());

//    this.opName = opName;
//    this.data = data;

    // block the commit phase until all the servers prepare
    this.allServersPreparedOperation = this.getAllowCommitLatch();
    // allow interested watchers to block until we are done with the commit phase
    this.allServersCompletedOperation = this.getCommitFinishedLatch();
 
  }

  // TEST ONLY
  public ThreePhaseCommit(ExceptionCheckable<DistributedCommitException> monitor, DistributedErrorListener errorListener, long wakeFrequency,
      long timeout) {
    this(monitor, errorListener, wakeFrequency);
    //    this.timer = setupTimer(errorListener, timeout);
  }

//  // Test ONLY
//  public ThreePhaseCommit(ExceptionCheckable<DistributedCommitException> monitor, DistributedErrorListener errorListener,
//      long wakeFrequency, int numPrepare,
//      int numAllowCommit, int numCommitted, int numCompleted, long timeout) {
//    this(monitor, monitor, errorListener, numPrepare, numAllowCommit, numCommitted, numCompleted, timeout);
//  }
//  
  /**
   * Setup the process timeout. The timer is started whenever {@link #call()} or {@link #run()} is
   * called.
   * @param errorListener
   * @param timeoutInfo information to pass along when the timer expires
   * @param timeout max amount of time the operation is allowed to run before expiring.
   * @return a timer for the overal operation
   */
  private OperationAttemptTimer setupTimer(final DistributedErrorListener errorListener,
      long timeout) {
    // setup a simple monitor so we pass through the error to the specific listener
    ExceptionSnare<OperationAttemptTimeoutException> passThroughMonitor = new ExceptionSnare<OperationAttemptTimeoutException>() {
      @Override
      public void receiveError(String msg, OperationAttemptTimeoutException cause, Object... info) {
        errorListener.operationTimeout(cause);
      }
    };
    return new OperationAttemptTimer(passThroughMonitor, timeout);
  }

  
  @Override
  public Void call() {
    LOG.debug("Starting three phase commit.");
    // start the timer
    this.timer.start();
    // run the operation
    this.call2();
    // tell the timer we are done, if we get here successfully
    this.timer.complete();
    return null;
  }

  public CountDownLatch getPreparedLatch() {
    return this.preparedLatch;
  }

  public CountDownLatch getAllowCommitLatch() {
    return this.allowCommitLatch;
  }

  public CountDownLatch getCommitFinishedLatch() {
    return this.commitFinishLatch;
  }

  public CountDownLatch getCompletedLatch() {
    return this.completedLatch;
  }

  public ExceptionCheckable<DistributedCommitException> getErrorCheckable() {
    return this.errorMonitor;
  }

  @SuppressWarnings("unchecked")
  public Void call2() {
    try {
      // start by checking for error first
      errorMonitor.failOnError();
      LOG.debug("Starting 'prepare' stage of two phase commit");
      phase = CommitPhase.PREPARE;
      prepare();

      // notify that we are prepared to snapshot
      LOG.debug("Prepare stage completed, counting down prepare.");
      this.getPreparedLatch().countDown();

      // wait for the indicator that we should commit
      LOG.debug("Waiting on 'commit allowed' latch to release.");
      phase = CommitPhase.PRE_COMMIT;

      // wait for the commit allowed latch to release
      waitForLatch(getAllowCommitLatch(), "commit allowed");
      errorMonitor.failOnError();

      LOG.debug("'Commit allowed' latch released, running commit step.");
      phase = CommitPhase.COMMIT;
      commit();

      // make sure we didn't get an error in commit
      phase = CommitPhase.POST_COMMIT;
      errorMonitor.failOnError();
      LOG.debug("Commit phase completed, counting down finsh latch.");
      this.getCommitFinishedLatch().countDown();
    } catch (Exception e) {
      // TODO I don't trust this exception catch and forced cast
      LOG.error("Two phase commit failed!", e);
      errorListener.localOperationException(phase, e);
      LOG.debug("Running cleanup phase.");
      this.cleanup(e);
    } finally {
      LOG.debug("Running finish phase.");
      this.finish();
      this.getCompletedLatch().countDown();
    }
    return null;
  }

  public void run() {
    this.call();
  }

  public DistributedErrorListener getErrorListener() {
    return errorListener;
  }

  /**
   * Wait for latch to count to zero, ignoring any spurious wake-ups, but waking periodically to
   * check for errors
   * @param latch latch to wait on
   * @param latchType String description of what the latch does
   * @throws E if the task was failed while waiting
   * @throws InterruptedException if we are interrupted while waiting for exception
   */
  public void waitForLatch(CountDownLatch latch, String latchType) throws DistributedCommitException, InterruptedException {
    Threads.waitForLatch(latch, errorMonitor, wakeFrequency, latchType);
  }


  public void prepare() throws DistributedCommitException {
    // start the operation
    LOG.debug("running the prepare phase, kicking off prepare phase on cohort.");
    try {
      // prepare the operation, cloning the list to avoid a concurrent modification from the
      // controller setting the prepared nodes
      controller.prepareOperation(opName, data, Lists.newArrayList(this.prepareNodes));
    } catch (IOException e) {
      parent.controllerConnectionFailure("Can't reach controller.", e);
    } catch (IllegalArgumentException e) {
      throw new DistributedCommitException(e, new byte[0]);
    }
  }

  public void commit() throws DistributedCommitException {
    try {
      // run the commit operation on the cohort
      controller.commitOperation(opName, Lists.newArrayList(this.commitingNodes));
    } catch (IOException e) {
      parent.controllerConnectionFailure("Can't reach controller.", e);
    }

    // then wait for the finish latch
    try {
      this.waitForLatch(this.allServersCompletedOperation, "cohort committed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DistributedCommitException(e, new byte[0]);
    }
  }

  /**
   * Subclass hook for custom cleanup/rollback functionality - currently, a no-op.
   */
  public void cleanup(Exception e) {
  }

  public void finish() {
    // remove ourselves from the running operations
    LOG.debug("Finishing coordinator operation - removing self from list of running operations");
    // then clear out the operation nodes
    LOG.debug("Reseting operation!");
    try {
      controller.resetOperation(opName);
    } catch (IOException e) {
      parent.controllerConnectionFailure("Failed to reset operation:" + opName, e);
    }
  }

  public void prepared(String node) {
    LOG.debug("node: '" + node + "' joining prepared barrier for operation '" + opName
        + "' on coordinator");
    if (this.prepareNodes.contains(node)) {
      synchronized (joinBarrierLock) {
        if (this.prepareNodes.remove(node)) {
          this.commitingNodes.add(node);
          this.allServersPreparedOperation.countDown();
        }
      }
      LOG.debug("Waiting on: " + allServersPreparedOperation
          + " remaining nodes to prepare operation");
    } else {
      LOG.debug("Node " + node + " joined operation, but we weren't waiting on it to join.");
    }
  }

  public void committed(String node) {
    boolean removed = false;
    synchronized (joinBarrierLock) {
      removed = this.commitingNodes.remove(node);
      this.allServersCompletedOperation.countDown();
    }
    if (removed) {
      LOG.debug("Node: '" + node + "' committed, counting down latch");
    } else {
      LOG.warn("Node: '" + node + "' committed operation, but we weren't waiting on it to commit.");
    }
  }

  
  
}