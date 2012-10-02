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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.apache.hadoop.hbase.util.Threads;

/**
 * A two-phase commit that enforces a time-limit on the operation. If the time limit expires before
 * the operation completes, the {@link ThreePhaseCommitErrorListenable} will receive a
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
public abstract class ThreePhaseCommit<L extends ThreePhaseCommitErrorListenable<E>, E extends Exception> 
  implements  Callable<Void>, Runnable, TwoPhaseCommitable<E>{

  /** latch counted down when the prepared phase completes */
  private final CountDownLatch preparedLatch;
  /** waited on before allowing the commit phase to proceed */
  private final CountDownLatch allowCommitLatch;
  /** counted down when the commit phase completes */
  private final CountDownLatch commitFinishLatch;
  /** counted down when the {@link #finish()} phase completes */
  private final CountDownLatch completedLatch;
  /** monitor to check for errors */
  private final ExceptionCheckable<E> errorMonitor;
  /** listener to listen for any errors to the operation */
  private final L errorListener;
  /** The current phase the operation is in */
  protected CommitPhase phase = CommitPhase.PRE_PREPARE;
  /** frequency to check for errors (ms) */
  protected final long wakeFrequency;
  
  private static final Log LOG = LogFactory.getLog(ThreePhaseCommit.class);
  protected final OperationAttemptTimer timer;

  /**
   * Constructor that has prepare, commit and finish latch counts of 1.
   * @param monitor notified if there is an error in the commit
   * @param errorListener listener to listen for errors to the running operation
   * @param wakeFrequency frequency to wake to check if there is an error via the monitor (in
   *          milliseconds).
   */
  public ThreePhaseCommit(ExceptionCheckable<E> monitor, L errorListener,
      long wakeFrequency) {
    // Default to a very large timeout
    this(monitor, errorListener, wakeFrequency, 1, 1, 1, 1, Integer.MAX_VALUE);
  }

  
  public ThreePhaseCommit(ExceptionCheckable<E> monitor, L errorListener,
      long wakeFrequency, int numPrepare,
      int numAllowCommit, int numCommitted, int numCompleted, long timeout) {
    this.errorMonitor = monitor;
    this.errorListener = errorListener;
    this.wakeFrequency = wakeFrequency;
    this.preparedLatch = new CountDownLatch(numPrepare);
    this.allowCommitLatch = new CountDownLatch(numAllowCommit);
    this.commitFinishLatch = new CountDownLatch(numCommitted);
    this.completedLatch = new CountDownLatch(numCompleted);
    this.timer = setupTimer(errorListener, timeout);
  }

  /**
   * Create a Three-Phase Commit operation
   * @param monitor error monitor to check for errors to the running operation
   * @param errorListener error listener to listen for errors while running the task
   * @param wakeFrequency frequency to check for errors while waiting for latches
   */
  public ThreePhaseCommit(ExceptionCheckable<E> monitor, L errorListener, long wakeFrequency,
      long timeout) {
    this(monitor, errorListener, wakeFrequency);
//    this.timer = setupTimer(errorListener, timeout);
  }

  /**
   * Create a Three-Phase Commit operation
   * @param numPrepare number of prepare latches (called when the prepare phase completes
   *          successfully)
   * @param numAllowCommit number of latches to wait on for the commit to proceed (blocks calls to
   *          the commit phase)
   * @param numCommitted number of commit completed latches should be established (called when the
   *          commit phase completes)
   * @param numCompleted number of completed latches to be established (called when everything has
   *          run)
   * @param monitor eror monitor to check for errors to the running operation
   * @param errorListener listener to listen for any errors to the running operation
   * @param wakeFrequency frequency to check for errors while waiting for latches
   * @param timeout max amount of time to allow for the operation to run
   */
  public ThreePhaseCommit(int numPrepare, int numAllowCommit, int numCommitted, int numCompleted,
      ExceptionCheckable<E> monitor, L errorListener, long wakeFrequency, long timeout) {
    this(monitor, errorListener, wakeFrequency, numPrepare, numAllowCommit, numCommitted,
        numCompleted, timeout);
  }

  /**
   * Setup the process timeout. The timer is started whenever {@link #call()} or {@link #run()} is
   * called.
   * @param errorListener
   * @param timeoutInfo information to pass along when the timer expires
   * @param timeout max amount of time the operation is allowed to run before expiring.
   * @return a timer for the overal operation
   */
  private OperationAttemptTimer setupTimer(final ThreePhaseCommitErrorListenable<E> errorListener,
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
  @Override
  public CountDownLatch getPreparedLatch() {
    return this.preparedLatch;
  }

  @Override
  public CountDownLatch getAllowCommitLatch() {
    return this.allowCommitLatch;
  }

  @Override
  public CountDownLatch getCommitFinishedLatch() {
    return this.commitFinishLatch;
  }

  @Override
  public CountDownLatch getCompletedLatch() {
    return this.completedLatch;
  }

  public ExceptionCheckable<E> getErrorCheckable() {
    return this.errorMonitor;
  }

  @Override
  public abstract void prepare() throws E;

  @Override
  public abstract void commit() throws E;

  @Override
  public abstract void cleanup(Exception e);


  @Override
  public abstract void finish();

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
      LOG.error("Two phase commit failed!", e);
      errorListener.localOperationException(phase, (E) e);
      LOG.debug("Running cleanup phase.");
      this.cleanup(e);
    } finally {
      LOG.debug("Running finish phase.");
      this.finish();
      this.getCompletedLatch().countDown();
    }
    return null;
  }

  @Override
  public void run() {
    this.call();
  }

  public L getErrorListener() {
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
  public void waitForLatch(CountDownLatch latch, String latchType) throws E, InterruptedException {
    Threads.waitForLatch(latch, errorMonitor, wakeFrequency, latchType);
  }

  
}