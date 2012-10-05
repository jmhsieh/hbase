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
import org.apache.hadoop.hbase.server.commit.distributed.controller.DistributedCommitCoordinatorController;
import org.apache.hadoop.hbase.server.commit.distributed.coordinator.DistributedThreePhaseCommitCoordinator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * ZooKeeper based {@link DistributedCommitCoordinatorController} for a
 * {@link DistributedThreePhaseCommitCoordinator}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ZKTwoPhaseCommitCoordinatorController implements DistributedCommitCoordinatorController {

  public static final Log LOG = LogFactory.getLog(ZKCommitUtil.class);

  private ZKCommitUtil zkController;
  protected final DistributedThreePhaseCommitCoordinator listener;
  
  /**
   * @param watcher zookeeper watcher. Owned by <tt>this</tt> and closed via {@link #close()}
   * @param operationDescription general description of the operation to use as the controlling
   *          znode
   * @param nodeName name of the node running the coordinator
   * @throws KeeperException if an unexpected zk error occurs
   */
  public ZKTwoPhaseCommitCoordinatorController(DistributedThreePhaseCommitCoordinator l, ZooKeeperWatcher watcher,
      String operationDescription, String nodeName) throws KeeperException {
    this.listener = l;
    // TODO need to make snanpshotZKUtil have a start method called by start in this class's start method.
    this.zkController = new ZKCommitUtil(watcher, operationDescription, nodeName) {
      @Override
      public void nodeCreated(String path) {
        if (!zkController.isInCommitPath(path)) return;
        LOG.debug("Node created: " + path);
        logZKTree(this.baseZNode);
        if (zkController.isPreparePathNode(path)) {
          // node wasn't present when we created the watch so zk event triggers prepare
          listener.prepared(ZKUtil.getNodeName(ZKUtil.getParent(path)), ZKUtil.getNodeName(path));
        }
        if (zkController.isCommitPathNode(path)) {
          // node wasn't present when we created the watch so zk envent triggers the commit.

          // TODO Nothing enforces that prepare and commits nodes from showing up in the wrong order. 
          listener.committed(ZKUtil.getNodeName(ZKUtil.getParent(path)), ZKUtil.getNodeName(path));
        }
        if (zkController.isAbortPathNode(path)) {
          abort(path);
        }
      }
    };
    zkController.clearChildNodes();
  }

  /**
   * The "prepare" phase.  The coordinator creates a new operation/prepare/ znode dir, if nodes
   * appear, first a prepare to relevant listener or sets watch waiting for notification of
   * the prep node
   * 
   * throws IOException if any failure occurs.
   */
  @Override
  public void prepareOperation(String operationName, byte[] info, List<String> nodeNames)
      throws IOException, IllegalArgumentException {
    // start watching for the abort node
    String abortNode = zkController.getAbortNode(operationName);
    try {
      // check to see if the abort node already exists
      if (ZKUtil.watchAndCheckExists(zkController.getWatcher(), abortNode)) {
        abort(abortNode);
      }
      // TODO What happens if we get the abort node watch triggered here?  We are aborting, going to
      // handle it but go about creating a new prepare barrier node.
      
    } catch (KeeperException e) {
      LOG.error("Failed to create abort", e);
      throw new IOException("Failed while watching abort node:" + abortNode, e);
    }

    // create the prepare barrier
    String prepare = zkController.getPrepareBarrierNode(operationName);
    LOG.debug("Creating prepare znode:" + prepare);
    try {
      // notify all the operation listeners to look for the prepare node
      ZKUtil.createSetData(zkController.getWatcher(), prepare, info);
      // loop through all the children of the prepare phase and watch for them
      for (String node : nodeNames) {
        String znode = ZKUtil.joinZNode(prepare, node);
        LOG.debug("Watching for prepare node:" + znode);
        if (ZKUtil.watchAndCheckExists(zkController.getWatcher(), znode)) {
          listener.prepared(operationName, node);
        }
      }
    } catch (KeeperException e) {
      throw new IOException("Failed while creating prepare node:" + prepare, e);
    }
  }

  @Override
  public void commitOperation(String operationName, List<String> nodeNames) throws IOException {
    String commit = zkController.getCommitBarrierNode(operationName);
    LOG.debug("Creating commit operation zk node:" + commit);
    try {
      // create the commit node and watch for the commit nodes
      ZKUtil.createAndFailSilent(zkController.getWatcher(), commit);
      // loop through all the children of the prepare phase and watch for them
      for (String node : nodeNames) {
        String znode = ZKUtil.joinZNode(commit, node);
        if (ZKUtil.watchAndCheckExists(zkController.getWatcher(), znode)) {
          listener.committed(operationName, node);
        }
      }
    } catch (KeeperException e) {
      throw new IOException("Failed while creating commit node:" + commit, e);
    }
  }


  /**
   * OperationName is prepared, commit, abort
   * 
   * // TODO this is named inconsistently -- operationName was snapshot id elsewhere. 
   */
  @Override
  public void resetOperation(String operationName) throws IOException {
    boolean stillGettingNotifications = false;
    do {
      try {
        LOG.debug("Attempting to clean out zk node for op:" + operationName);
        zkController.clearNodes(operationName);
        stillGettingNotifications = false;
      } catch (KeeperException.NotEmptyException e) {
        // recursive delete isn't transactional (yet) so we need to deal with cases where we get
        // children trickling in
        stillGettingNotifications = true;
      } catch (KeeperException e) {
        throw new IOException("Failed to complete reset operation", e);
      }
    } while (stillGettingNotifications);
  }

  /**
   * Start monitoring nodes in ZK - subclass hook to start monitoring nodes they are about.
   * 
   * // TODO Why isn't the the listener registered to ZK here instead of constructor?
   */
  public void start() {
    LOG.debug("Starting the commit controller for operation:" + zkController.nodeName);
    // NOOP - used by subclasses to start monitoring things they care about
  }
  
  @Deprecated
  public void start(DistributedThreePhaseCommitCoordinator listener) {
    //// Made listener final and force it to be set in the constructor.
    // this.listener = listener;
    this.start();
  }


  /**
   * This this is the abort message being sent by the coordinator to cohort
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
      zkController.logZKTree(zkController.baseZNode);
      listener.controllerConnectionFailure("Failed to post zk node:" + operationAbortNode
          + " to abort operation", new IOException(e));
    }
  }

  /**
   * Pass along the found abort notification to the listener
   * @param abortNode full znode path to the failed operation information
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

  @Override
  public void close() throws IOException {
    zkController.close();
  }

  public ZKCommitUtil getZkController() {
    return zkController;
  }
}