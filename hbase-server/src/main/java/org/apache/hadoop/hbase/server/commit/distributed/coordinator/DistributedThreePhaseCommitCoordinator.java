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
package org.apache.hadoop.hbase.server.commit.distributed.coordinator;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
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
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitCoordinatorController;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * General coordinator for a set of three-phase-commit operations (tasks).
 * <p>
 * This coordinator is used to launch multiple tasks of the same type (e.g. a snapshot) with
 * differences per instance specified via {@link #kickOffCommit(String, byte[], List)}.
 * <p>
 * NOTE: Tasks are run concurrently. To ensure a serial ordering of tasks, synchronize off the
 * completion of the task returned from {@link #kickOffCommit(String, byte[], List)}.
 * <p>
 * By default, a generic {@link CoordinatorTask} will be used to run the requested task, but a
 * custom task can be specified by a {@link CoordinatorTaskBuilder}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DistributedThreePhaseCommitCoordinator {
  private static final Log LOG = LogFactory.getLog(DistributedThreePhaseCommitCoordinator.class);

  private DistributedCommitCoordinatorController controller;
  private CoordinatorTaskBuilder builder;
//  private final DistributedThreePhaseCommitManager manager;
  private final Map<String, RunningOperation> operations = new HashMap<String, RunningOperation>();
  private final ExecutorService pool;
  protected final RemoteExceptionSerializer serializer;
  
  public DistributedThreePhaseCommitCoordinator(String nodeName,
      long keepAliveTime,
 int opThreads,
      long wakeFrequency, DistributedCommitCoordinatorController controller,
      CoordinatorTaskBuilder builder) {
    this(nodeName, new ThreadPoolExecutor(0, opThreads, keepAliveTime,
        TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new DaemonThreadFactory("(" + nodeName
            + ")-3PC commit-pool")));
    this.controller = controller;
    setBuilder(builder);
  }
  

  /**
   * Create a manager with the specified controller, name and pool.
   * <p>
   * Exposed for testing!
   * @param controller control access to the rest of the cluster
   * @param nodeName name of the node
   * @param pool thread pool to which new tasks are submitted
   */
  public DistributedThreePhaseCommitCoordinator(String nodeName, ThreadPoolExecutor pool) {
    this.serializer = new RemoteExceptionSerializer(nodeName);
    this.pool = pool;
  }

  /**
   * Create a manager with the specified controller, node name and parameters for a standard thread
   * pool
   * @param controller control access to the rest of the cluster
   * @param nodeName name of the node where <tt>this</tt> is running
   * @param keepAliveTime amount of time (ms) idle threads should be kept alive
   * @param opThreads number of threads that should be used for running tasks
   * @param poolName prefix name that should be used for the pool
   */
  public DistributedThreePhaseCommitCoordinator(String nodeName, long keepAliveTime,
      int opThreads, String poolName) {
    this(nodeName, new ThreadPoolExecutor(0, opThreads, keepAliveTime,
        TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new DaemonThreadFactory("(" + poolName
            + ")-3PC commit-pool")));
  }

  /**
   * TODO: THIS IS A HACK until it is refactored.  We should not expose this.
   * 
   * Actions on this reference need to be sychronized.
   * 
   * @return
   */
  public Map<String, RunningOperation> getOperationsRef() {
    return operations;
  }
  
  public void close() {
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
    for (Entry<String, RunningOperation> e: toNotify.entrySet()) {
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
    DistributedErrorListener listener = running.getErrorListener();
    if (listener != null) {
      listener.remoteCommitError(remote[0]);
    }
  }

  /**
   * A weak reference to in-flight commits
   */
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

  
  public void setController(DistributedCommitCoordinatorController dcc) {
    this.controller = dcc;
  }

  public void setBuilder(CoordinatorTaskBuilder builder) {
    this.builder = builder;
  }

  /**
   * Kick off the named operation.
   * @param operationName name of the operation to start
   * @param operationInfo information for the operation to start
   * @param expectedNodes expected nodes to start
   * @return handle to the running operation, if it was started correctly, <tt>null</tt> otherwise
   * @throws RejectedExecutionException if there are no more available threads to run the operation
   */
  public ThreePhaseCommit kickOffCommit(String operationName, byte[] operationInfo,
      List<String> expectedNodes) throws RejectedExecutionException {
    // build the operation
    ThreePhaseCommit commit = builder.buildOperation(this, operationName, operationInfo,
      expectedNodes);
    if (this.submitOperation(commit.getErrorListener(), operationName, commit)) {
      return commit;
    }
    return null;
  }

  /**
   * Notification that the operation had another node finished preparing the running operation
   * @param operationName name of the operation that prepared
   * @param node name of the node that prepared
   */
  public void prepared(String commitName, final String node) {    
    RunningOperation running = getOperation(commitName);
    ThreePhaseCommit op = running.getOp();
    if (op != null) {
      op.prepared(node);
    }
  }

  /**
   * Notification that the operation had another node finished committing the running operation
   * @param commitName name of the operation that finished
   * @param node name of the node that committed
   */
  public void committed(String commitName, final String node) {
    RunningOperation running = getOperation(commitName);
    ThreePhaseCommit op = running.getOp();
    if (op != null) {
      op.committed(node);
    }
  }

  /**
   * @return the controller for all current operations
   */
  public DistributedCommitCoordinatorController getController() {
    return controller;
  }
}