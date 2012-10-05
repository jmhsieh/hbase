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
package org.apache.hadoop.hbase.server.commit.distributed.cohort;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.protobuf.generated.DistributedCommitProtos.CommitPhase;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.commit.ThreePhaseCommit;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedCommitException;
import org.apache.hadoop.hbase.server.commit.distributed.DistributedErrorListener;
import org.apache.hadoop.hbase.server.commit.distributed.RemoteExceptionSerializer;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.apache.hadoop.hbase.util.Threads;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Process to kick off and manage a running {@link ThreePhaseCommit} cohort member. This is the part
 * of the three-phase commit that actually does the work and reports back to the coordinator when it
 * completes each phase.
 * <p>
 * If there is a connection error ({@link #controllerConnectionFailure(String, IOException)}) all
 * currently running operations are failed since we no longer can reach any other members as the
 * controller is down.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DistributedThreePhaseCommitCohortMember implements Closeable {
  private static final Log LOG = LogFactory.getLog(DistributedThreePhaseCommitCohortMember.class);

  private final CohortMemberTaskBuilder builder;
  private final long wakeFrequency;
  private final RemoteExceptionSerializer serializer;
  private final DistributedCommitCohortMemberController controller;

  private final Map<String, RunningOperation> operations = new HashMap<String, RunningOperation>();
  private final ExecutorService pool;

  /**
   * @param wakeFrequency frequency in ms to check for errors in the operation
   * @param keepAlive amount of time to keep alive idle worker threads
   * @param opThreads max number of threads to use for running operations. In reality, we will
   *          create 2X the number of threads since we need to also run a monitor thread for each
   *          running operation
   * @param controller controller used to send notifications to the operation coordinator
   * @param builder build new distributed three phase commit operations on demand
   * @param nodeName name of the node to use when notifying the controller that an operation has
   *          prepared, committed, or aborted on operation
   */
  public DistributedThreePhaseCommitCohortMember(long wakeFrequency, long keepAlive, int opThreads,
      DistributedCommitCohortMemberController controller,
      CohortMemberTaskBuilder builder, String nodeName) {
    this.pool = new ThreadPoolExecutor(0, opThreads, keepAlive,
        TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new DaemonThreadFactory("( cohort-memeber-" + nodeName
            + ")-3PC commit-pool"));
    this.controller = controller;
    this.builder = builder;
    this.wakeFrequency = wakeFrequency;
    this.serializer = new RemoteExceptionSerializer(nodeName);
  }

  /**
   * Exposed for testing!
   * @param controller controller used to send notifications to the operation coordinator
   * @param nodeName name of the node to use when notifying the controller that an operation has
   *          prepared, committed, or aborted on operation
   * @param pool thread pool to submit tasks
   * @param builder build new distributed three phase commit operations on demand
   * @param wakeFrequency frequency in ms to check for errors in the operation
   */
  public DistributedThreePhaseCommitCohortMember(
      DistributedCommitCohortMemberController controller, String nodeName, ThreadPoolExecutor pool,
      CohortMemberTaskBuilder builder, long wakeFrequency) {
    this.serializer = new RemoteExceptionSerializer(nodeName);
    this.pool = pool;
    this.controller = controller;
    this.builder = builder;
    this.wakeFrequency = wakeFrequency;
  }

  /**
   * Gets an operation from currently running operation list.  
   * 
   * WTF: Removes if is removable?
   * 
   * @param opName
   * @return
   */
  RunningOperation getOperation(String opName) {
    RunningOperation running;
    synchronized (operations) {
      running = operations.get(opName);
      if (running == null) {
        LOG.warn("Don't know about op:" + opName+ ", ignoring notification attempt.");
        return null;
      }
      if (running.isRemovable()) {
        operations.remove(opName);
        return null;
      }
    }
    running.logMismatchedNulls();
    return running;
  }
  
  /**
   * Creates a new commit object, gets listener from commit and then registers a new monitor.
   * 
   * @param opName
   * @param data
   */
  public void runNewOperation(String opName, byte[] data) {
    // build a new operation
    ThreePhaseCommit commit = null;
    try {
      commit = builder.buildNewOperation(opName, data);
    } catch (IllegalArgumentException e) {
      abortOperationAttempt(opName, e);
    } catch (IllegalStateException e) {
      abortOperationAttempt(opName, e);
    }
    if (commit == null) {
      LOG.info("Operation:" + opName + " doesn't have a local task, not anything running here.");
      return;
    }
    // create a monitor to watch for operation failures
    Monitor monitor = new Monitor(opName, commit, commit.getErrorCheckable());
    DistributedErrorListener dispatcher = commit.getErrorListener();
    // make sure the listener watches for errors to running operation
    dispatcher.addErrorListener(monitor);
    LOG.debug("Submitting new operation:" + opName);
    if (!this.submitOperation(dispatcher, opName, commit, Executors.callable(monitor))) {
      LOG.error("Failed to start operation:" + opName);
    }
  }

  /**
   * Attempt to abort the operation globally because of a specific exception.
   * @param opName name of the operation to fail
   * @param cause reason the operation failed
   * @throws RuntimeException if the controller throws an {@link IOException}
   */
  private void abortOperationAttempt(String opName, Exception cause) {
    LOG.error("Failed to start op: " + opName + ", failing globally.");
    try {
      this.controller.abortOperation(opName, this.serializer.buildRemoteException(cause));
    } catch (IOException e) {
      LOG.error("Failed to abort operation!");
      throw new RuntimeException(e);
    }
  }

  /**
   * Notification that operation coordinator has reached the committed phase
   * @param opName name of the operation that should start running the commit phase
   */
  public void commitInitiated(String opName) {
    RunningOperation running = getOperation(opName);
    ThreePhaseCommit op = running.getOp();
    if (op != null) {
      op.getAllowCommitLatch().countDown();
    }
  }

  /**
   * Monitor thread for a running operation. Its responsible for tracking the progress of the local
   * operation progress. In the case of all things working correctly, the {@link Monitor} just waits
   * on the task's progress and notifies a {@link DistributedCommitCohortMemberController} of cohort
   * member's progress.
   * <p>
   * However, the monitor also tracks the errors the task encounters. The {@link Monitor} is bound
   * as a generic {@link DistributedErrorListener} to the operation's
   * {@link ErrorMonitorable}, allowing it to get updates when the operation encounters errors.
   * <p>
   * Local errors are serialized and then propagated to the
   * {@link DistributedCommitCohortMemberController} so the controller and the rest of the cohort
   * can see the error and kill themselves in a timely manner (rather than waiting for the task
   * timer).
   * <p>
   * Remote errors already reach the {@link ErrorMonitorable} - failing the running operation -
   * allowing the {@link Monitor} to disregard calls to
   * {@link #remoteCommitError(RemoteFailureException)}. A similar logic also holds for calls to
   * {@link #controllerConnectionFailure(String, IOException)} (further, it doesn't make sense to
   * update the controller because it is won't reach any other cohort members).
   */
  private class Monitor extends Thread implements DistributedErrorListener {

    private String opName;
    private CountDownLatch prepared;
    private CountDownLatch commit;
    private ExceptionCheckable<DistributedCommitException> errorChecker;

    public Monitor(String opName, ThreePhaseCommit operation,
        ExceptionCheckable<DistributedCommitException> checkable) {
      this.opName = opName;
      this.prepared = operation.getPreparedLatch();
      this.commit = operation.getCommitFinishedLatch();
      this.errorChecker = checkable;
    }

    @Override
    public void run() {
      try {
        LOG.debug("Running operation '" + opName + "' monitor, waiting for prepared");
        // wait for the prepared latch
        Threads.waitForLatch(prepared, errorChecker, wakeFrequency, "monitor waiting on operation ("
            + opName + ") prepared ");
        LOG.debug("prepared latch finished, notifying controller");
        // then notify the controller that we are prepared
        controller.prepared(opName);

        // then wait for the committed latch
        LOG.debug("Notified controller that we are prepared, waiting on commit latch.");
        Threads.waitForLatch(commit, errorChecker, wakeFrequency, "monitor waiting on operation ("
            + opName + ") committed");

        // notify the controller that we committed the operation
        controller.commited(opName);
        LOG.debug("Notified controller that we committed");
        // we can ignore the exceptions found while waiting because they will be passed to us as an
        // error listener for the general operation. Still logging them because its good to see the
        // propagation
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting - pool is shutdown. Killing monitor and task.");
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error("Task '" + opName + "' failed.", e);
      }
    }

    @Override
    public void localOperationException(CommitPhase phase, Exception cause) {
      LOG.error("Got a local opeation error, notifying controller");
      abort(serializer.buildRemoteException(phase, cause));
    }

    @Override
    public void operationTimeout(OperationAttemptTimeoutException cause) {
      LOG.error("Got a local opeation timeout, notifying controller");
      abort(serializer.buildRemoteException(cause));
    }

    /**
     * Pass an abort notification onto the actual controller to abort the operation
     * @param exceptionSerializer general exception class
     */
    private void abort(RemoteFailureException failureMessage) {
      try {
        controller.abortOperation(opName, failureMessage);
      } catch (IOException e) {
        // this will fail all the running operations, since the connection is down
        controllerConnectionFailure("Failed to abort operation:" + opName, e);
      }
    }

    @Override
    public void controllerConnectionFailure(String message, IOException cause) {
      LOG.error("Can't reach controller, not propagting error", cause);
    }

    @Override
    public void remoteCommitError(RemoteFailureException remoteCause) {
      LOG.error("Remote commit failure, not propagating error:" + remoteCause);
    }

    @Override
    public void addErrorListener(DistributedErrorListener listener) {
      throw new UnsupportedOperationException("Cohort member monitor can't add listeners.");
    }
  }

  @Override
  public void close() throws IOException {
    // have to use shutdown now to break any latch waiting
    pool.shutdownNow();
  }


  /**
   * Submit an operation and all its dependent operations to be run.
   * @param errorMonitor monitor to notify if we can't start all the operations
   * @param operationName operation to start (ex: a snapshot id)
   * @param tasks subtasks of the primary operation
   * @return <tt>true</tt> if the operation was started correctly, <tt>false</tt> if the primary
   *         task or any of the sub-tasks could not be started. In the latter case, if the pool is
   *         full the error monitor is also notified that the task could not be created via a
   *         {@link DistributedErrorListener#localOperationException(CommitPhase, Exception)}
   */
  public boolean submitOperation(DistributedErrorListener errorMonitor, String operationName, ThreePhaseCommit primary,
      Callable<?>... tasks) {
    // if the submitted task was null, then we don't want to run the subtasks
    if (primary == null) return false;

    // make sure we aren't already running an operation of that name
    synchronized (operations) {
      if (operations.get(operationName) != null) return false;
    }

    // kick off all the tasks
    List<Future<?>> futures = new ArrayList<Future<?>>((tasks == null ? 0 : tasks.length) + 1);
    try {
      futures.add(this.pool.submit((Callable<?>) primary));
      for (Callable<?> task : tasks) {
        futures.add(this.pool.submit(task));
      }
      // if everything got started properly, we can add it known running operations
      synchronized (operations) {
        this.operations.put(operationName, new RunningOperation(primary, errorMonitor));
      }
      return true;
    } catch (RejectedExecutionException e) {
      // the thread pool is full and we can't run the operation
      errorMonitor.localOperationException(CommitPhase.PRE_PREPARE, new DistributedCommitException(
          "Operation pool is full!", e));
      // cancel all operations proactively
      for (Future<?> future : futures) {
        future.cancel(true);
      }
    }
    return false;
  }

  /**
   * The connection to the rest of the commit group (cohort and coordinator) has been
   * broken/lost/failed. This should fail any interested operations, but not attempt to notify other
   * members since we cannot reach them anymore.
   * @param message description of the error
   * @param cause the actual cause of the failure
   */
  public void controllerConnectionFailure(final String message, final IOException cause) {
    List<String> remove = new ArrayList<String>();
    Map<String, RunningOperation> toNotify = new HashMap<String, RunningOperation>();
    synchronized (operations) {
      for (Entry<String, RunningOperation> op : operations.entrySet()) {
        if (op.getValue().isRemovable()) {
          LOG.warn("Ignoring error notification for operation:" + op.getKey()
              + "  because we the op has finished already.");
          remove.add(op.getKey());
          continue;
        }
        toNotify.put(op.getKey(), op.getValue());
      }
    }
    for (Entry<String, RunningOperation> e : toNotify.entrySet()) {
      RunningOperation running = e.getValue();
      ThreePhaseCommit op = running.getOp();
      DistributedErrorListener listener = running.getErrorListener();
      running.logMismatchedNulls();
      if (op == null) {
        // if the op is null, we probably don't have any more references, so we should check again
        if (running.isRemovable()) {
          remove.add(e.getKey());
          continue;
        }
      }
      // notify the elements, if they aren't null
      if (listener != null) { 
        listener.controllerConnectionFailure(message, cause);
      }
    }
    
    // clear out operations that have finished
    synchronized (operations) {
      for (String op : remove) {
        operations.remove(op);
      }
    }
  }


  /**
   * Abort the operation with the given name
   * @param opName name of the operation to about
   * @param reason serialized information about the abort
   */
  public void abortOperation(String opName, byte[] reason) {
    // figure out the data we need to pass
    final RemoteFailureException[] remote = new RemoteFailureException[1];
    try {
      remote[0] = RemoteFailureException.parseFrom(reason);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Got an error notification for op:" + opName
          + " but we can't read the information. Killing the operation.");
      // we got a remote exception, but we can't describe it (pos, so just
      remote[0] = serializer.buildRemoteException(e);
    }
    // if we know about the operation, notify it
    RunningOperation running = getOperation(opName);
    DistributedErrorListener l = running.getErrorListener(); 
    if (l != null) {
      l.remoteCommitError(remote[0]);
    }
  }

  public static class RunningOperation {
    private final WeakReference<DistributedErrorListener> errorListener;
    private final WeakReference<ThreePhaseCommit> op;

    public RunningOperation(ThreePhaseCommit  op, DistributedErrorListener listener) {
      this.errorListener = new WeakReference<DistributedErrorListener>(listener);
      this.op = new WeakReference<ThreePhaseCommit>(op);
    }

    public boolean isRemovable() {
      return errorListener.get() == null && this.op.get() == null;
    }
    public ThreePhaseCommit getOp() {
      return op.get();
    }
    
    public DistributedErrorListener getErrorListener() {
      return errorListener.get();
    }

    protected void logMismatchedNulls() {
      if (op == null && errorListener != null || op != null && errorListener == null) {
        LOG.warn("Operation is currently null:" + (op == null) + ", but listener is "
            + (errorListener == null ? "" : "not") + "null -- Possible memory leak.");
      }
    }
  }

  /**
   * Exposed for testing!
   * @return get the underlying thread pool.
   */
  public ExecutorService getThreadPool() {
    return this.pool;
  }
}