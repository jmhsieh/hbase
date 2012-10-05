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
package org.apache.hadoop.hbase.server.commit.distributed.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.RemoteFailureException;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedCommitCohortMemberController;
import org.apache.hadoop.hbase.server.commit.distributed.cohort.DistributedThreePhaseCommitCohortMember;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * ZooKeeper based controller for a two-phase commit cohort member.
 * <p>
 * There can only be one {@link ZKTwoPhaseCommitCohortMemberController} per operation per node,
 * since each operation type is bound to a single set of nodes. You can have multiple
 * {@link ZKTwoPhaseCommitCohortMemberController} on the same server, each serving a different node
 * name, but each individual controller is still bound to a single node name (and since they are
 * used to determine global progress, its important to not get this wrong).
 * <p>
 * To make this slightly more confusing, you can run multiple, concurrent operations at the same
 * time (as long as they have different names), from the same controller, but the same node name
 * must be used for each operation (though there is no conflict between the two operations as long
 * as they have distinct names).
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ZKTwoPhaseCommitCohortMemberController implements DistributedCommitCohortMemberController {

  private static final Log LOG = LogFactory.getLog(ZKTwoPhaseCommitCohortMemberController.class);
  private final String nodeName;
  
  protected DistributedThreePhaseCommitCohortMember listener;
  private ZKCommitUtil zkController; 

  
  // TODO: on recovery we need to instantiate new instances of this for every outstanding
  // distributed commit?  Where is this recovery logic?
  
  /**
   * Must call {@link #start(DistributedThreePhaseCommitCohortMember)} before this is can be used.
   * @param watcher {@link ZooKeeperWatcher} to be owned by <tt>this</tt>. Closed via
   *          {@link #close()}.
   * @param operationDescription name of the znode describing the operation to run
   * @param nodeName name of the node to join the operation
   * @throws KeeperException if we can't reach zookeeper
   */
  public ZKTwoPhaseCommitCohortMemberController(ZooKeeperWatcher watcher,
      String operationDescription, String nodeName) throws KeeperException {
    this.zkController = new ZKCommitUtil(zkController.getWatcher(), operationDescription, nodeName) {
      @Override
      public void nodeCreated(String path) {
        if (path.startsWith(this.baseZNode)) {
          LOG.info("Received created event:" + path);
          // if it is a simple start/end/abort then we just rewatch the node
          if (path.equals(this.prepareBarrier)) {
            watchForNewOperations();
            return;
          } else if (path.equals(this.abortZnode)) {
            watchForAbortedOperations();
            return;
          }
          String parent = ZKUtil.getParent(path);
          // if its the end barrier, the operation can be completed
          if (parent.equals(this.commitBarrier)) {
            passAlongCommit(path);
            return;
          } else if (parent.equals(this.abortZnode)) {
            abort(path);
            return;
          } else if (parent.equals(this.prepareBarrier)) {
            startNewOperation(path);
          } else {
            LOG.debug("Ignoring created notification for node:" + path);
          }
        }
      }
      
      @Override
      public void nodeChildrenChanged(String path) {
        LOG.info("Received children changed event:" + path);
        if (path.equals(this.prepareBarrier)) {
          LOG.info("Recieved start event.");
          watchForNewOperations();
        } else if (path.equals(this.abortZnode)) {
          LOG.info("Recieved abort event.");
          watchForAbortedOperations();
        }
      }
    };
    this.nodeName = nodeName;
  }

  public ZKCommitUtil getZkController() {
    return zkController;
  }
  
  protected void start() {
    LOG.debug("Starting the cohort controller");
    watchForAbortedOperations();
    watchForNewOperations();
  }


  /**
   * Pass along the commit notification to any listeners
   * @param path full znode path that cause the notification
   */
  private void passAlongCommit(String path) {
    LOG.debug("Recieved committed event:" + path);
    String opName = ZKUtil.getNodeName(path);
    this.listener.commitInitiated(opName);
  }



  private void watchForAbortedOperations() {
    LOG.debug("Checking for aborted operations on node:" + zkController.abortZnode);
    try {
      // this is the list of the currently aborted operations
      for (String node : ZKUtil.listChildrenAndWatchForNewChildren(zkController.getWatcher(), zkController.abortZnode)) {
        String abortNode = ZKUtil.joinZNode(zkController.abortZnode, node);
        abort(abortNode);
      }
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to list children for abort node:"
          + zkController.abortZnode, new IOException(e));
    }
  }

  private void watchForNewOperations() {
    // watch for new operations that we need to start
    LOG.debug("Looking for new operations under znode:" + zkController.prepareBarrier);
    List<String> runningOperations = null;
    try {
      runningOperations = ZKUtil.listChildrenAndWatchForNewChildren(zkController.getWatcher(), zkController.prepareBarrier);
      if (runningOperations == null) {
        LOG.debug("No running operations.");
        return;
      }
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("General failure when watching for new operations",
        new IOException(e));
    }
    for (String operationName : runningOperations) {
      // then read in the operation information
      String path = ZKUtil.joinZNode(zkController.prepareBarrier, operationName);
      startNewOperation(path);
    }
  }

  /**
   * Kick off a new operation on the listener with the data stored in the passed znode.
   * <p>
   * Will attempt to create the same operation multiple times if an operation znode with the same
   * name is created. It is left up the coordinator to ensure this doesn't occur.
   * @param path full path to the znode for the operation to start
   */
  private synchronized void startNewOperation(String path) {
    LOG.debug("Found operation node: " + path);
    String opName = ZKUtil.getNodeName(path);
    // start watching for an abort notification for the operation
    String abortNode = zkController.getAbortNode(opName);
    try {
      if (ZKUtil.watchAndCheckExists(zkController.getWatcher(), abortNode)) {
        LOG.debug("Not starting:" + opName + " because we already have an abort notification.");
        return;
      }
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to get the abort node (" + abortNode
          + ") for operation:" + opName, new IOException(e));
      return;
    }

    // get the data for the operation
    try {
      byte[] data = ZKUtil.getData(zkController.getWatcher(), path);
      LOG.debug("Found data for znode:" + path);
      listener.runNewOperation(opName, data);
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to get data for new operation:" + opName,
        new IOException(e));
    }
  }

  /**
   * This attempts to create a prepared state node for the operation (snapshot name).  This acts as
   * the YES vote in 2pc.
   * 
   * It then looks for the commit znode that will act as the COMMIT message.  If not present we
   * have a watcher, if present then trigger the committed action.
   * 
   * TODO: Why no watch on the abort message?
   * TODO: What if this had already been committed? What prevents double commit?
   */
  @Override
  public void prepared(String operationName) throws IOException {
    try {
      LOG.debug("Node: '" + nodeName + "' joining prepared barrier for operation (" + operationName
          + ") in zk");
      String prepared = ZKUtil.joinZNode(ZKCommitUtil.getPrepareBarrierNode(zkController, operationName), nodeName);
      // TODO: this is seems concerning -- this effectively says we cannot log progress.
      ZKUtil.createAndFailSilent(zkController.getWatcher(), prepared);

      // watch for the complete node for this snapshot
      String commitBarrier = zkController.getCommitBarrierNode(operationName);
      LOG.debug("Starting to watch for commit barrier:" + commitBarrier);
      if (ZKUtil.watchAndCheckExists(zkController.getWatcher(), commitBarrier)) {
        passAlongCommit(commitBarrier);
      }
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to join the prepare barrier for operaton: "
          + operationName + " and node: " + nodeName, new IOException(e));
    }
  }

  /**
   * This acts as the ack for a commit.
   */
  @Override
  public void commited(String operationName) throws IOException {
    LOG.debug("Marking operation (" + operationName + ") committed for node '" + nodeName
        + "' in zk");
    String joinPath = ZKUtil.joinZNode(zkController.getCommitBarrierNode(operationName), nodeName);
    try {
      ZKUtil.createAndFailSilent(zkController.getWatcher(), joinPath);
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to post zk node:" + joinPath
          + " to join commit barrier.", new IOException(e));
    }
  }
  
  /**
   * This should be called by the cohort and writes an abort zk which acts as the ??? in 2pc
   * 
   * TODO: hm.. maybe this is this the NO vote?  
   * TODO: where is aborted -- when the coordinator sends ABORT to the cohort?
   */
  @Override
  public void abortOperation(String operationName, RemoteFailureException failureInfo) {
    LOG.debug("Aborting operation (" + operationName + ") in zk");
    String operationAbortNode = zkController.getAbortNode(operationName);
    try {
      LOG.debug("Creating abort node:" + operationAbortNode);
      byte[] errorInfo = failureInfo.toByteArray();
      // first create the znode for the operation
      ZKUtil.createSetData(zkController.getWatcher(), operationAbortNode, errorInfo);
      LOG.debug("Finished creating abort node:" + operationAbortNode);
    } catch (KeeperException e) {
      // possible that we get this error for the operation if we already reset the zk state, but in
      // that case we should still get an error for that operation anyways
      zkController.logZKTree(zkController.getBaseZnode());
      listener.controllerConnectionFailure("Failed to post zk node:" + operationAbortNode
          + " to abort operation", new IOException(e));
    }
  }


  /**
   * Pass along the found abort notification to the listener
   * @param abortNode full znode path to the failed operation information
   * 
   * TODO: does this only come from the coordinator?  
   * TODO: should this be name aborted (similar to prepared, committed)?
   */
  protected void abort(String abortNode) {
    String opName = ZKUtil.getNodeName(abortNode);
    try {
      byte[] data = ZKUtil.getData(zkController.getWatcher(), abortNode);
      this.listener.abortOperation(opName, data);
    } catch (KeeperException e) {
      listener.controllerConnectionFailure("Failed to get data for abort node:" + abortNode
          + zkController.getAbortZnode(), new IOException(e));
    }
  }

  public void start(DistributedThreePhaseCommitCohortMember listener) {
    LOG.debug("Starting the commit controller for operation:" + this.nodeName);
    this.listener = listener;
    this.start();
  }

  @Override
  public void close() throws IOException {
    zkController.close();
  }

}