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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * ZooKeeper based controller for a distributed two-phase commit
 * @param <L> type of listener to watch for errors and progress of an operation (both local and
 *          remote)
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class ZKTwoPhaseCommitController
    extends ZooKeeperListener implements Closeable {

  private static final Log LOG = LogFactory.getLog(ZKTwoPhaseCommitController.class);

  public static final String START_BARRIER_ZNODE = "prepare";
  public static final String END_BARRIER_ZNODE = "commit";
  public static final String ABORT_ZNODE = "abort";

  public final String baseZNode;
  protected final String prepareBarrier;
  protected final String commitBarrier;
  protected final String abortZnode;

  protected final String nodeName;

  /*
   * Layout of zk is
   * /hbase/[op name]/prepare/
   *                    [op instance] - op data/
   *                        /[nodes that have prepared]
   *                 /commit/
   *                    [op instance]/
   *                        /[nodes that have committed]
   *                /abort/
   *                    [op instance] - failure data/
   *  Assumption here that snapshot names are going to be unique
   */

  /**
   * Top-level watcher/controller for snapshots across the cluster.
   * <p>
   * On instantiation ensures the snapshot znodes exists, but requires calling {@link #start} to
   * start monitoring for running two phase commits.
   * @param watcher watcher for the cluster ZK. Owned by <tt>this</tt> and closed via
   *          {@link #close()}
   * @param operationDescription name of the znode describing the operation to run
   * @param nodeName name of the node from which we are interacting with running operations
   * @throws KeeperException when the operation znodes cannot be created
   */
  public ZKTwoPhaseCommitController(ZooKeeperWatcher watcher, String operationDescription,
      String nodeName) throws KeeperException {
    super(watcher);
    this.nodeName = nodeName;
    // make sure we are listening for events
    watcher.registerListener(this);
    // setup paths for the zknodes used in snapshotting
    this.baseZNode = ZKUtil.joinZNode(watcher.baseZNode, operationDescription);
    prepareBarrier = ZKUtil.joinZNode(baseZNode, START_BARRIER_ZNODE);
    commitBarrier = ZKUtil.joinZNode(baseZNode, END_BARRIER_ZNODE);
    abortZnode = ZKUtil.joinZNode(baseZNode, ABORT_ZNODE);

    // first make sure all the ZK nodes exist
    // make sure all the parents exist (sometimes not the case in tests)
    ZKUtil.createWithParents(watcher, prepareBarrier);
    // regular create because all the parents exist
    ZKUtil.createAndFailSilent(watcher, commitBarrier);
    ZKUtil.createAndFailSilent(watcher, abortZnode);
  }

  @Override
  public void close() throws IOException {
    if (watcher != null) {
      watcher.close();
    }
  }

  public String getPrepareBarrierNode(String opInstanceName) {
    return ZKTwoPhaseCommitController.getPrepareBarrierNode(this, opInstanceName);
  }

  public String getCommitBarrierNode(String opInstanceName) {
    return ZKTwoPhaseCommitController.getCommitBarrierNode(this, opInstanceName);
  }

  public String getAbortNode(String opInstanceName) {
    return ZKTwoPhaseCommitController.getAbortNode(this, opInstanceName);
  }

  public String getAbortZnode() {
    return abortZnode;
  }

  public String getBaseZnode() {
    return baseZNode;
  }
  
  /**
   * Get the full znode path to the node used by the coordinator to starting the operation and as a
   * barrier node for the prepare phase.
   * @param controller controller running the operation
   * @param opInstanceName name of the running operation instance (not the operation description).
   * @return full znode path to the prepare barrier/start node
   */
  public static String getPrepareBarrierNode(ZKTwoPhaseCommitController controller,
      String opInstanceName) {
    return ZKUtil.joinZNode(controller.prepareBarrier, opInstanceName);
  }

  /**
   * Get the full znode path to the node used by the coordinator as a barrier for the commit phase.
   * @param controller controller running the operation
   * @param opInstanceName name of the running operation instance (not the operation description).
   * @return full znode path to the commit barrier
   */
  public static String getCommitBarrierNode(ZKTwoPhaseCommitController controller,
      String opInstanceName) {
    return ZKUtil.joinZNode(controller.commitBarrier, opInstanceName);
  }

  /**
   * Get the full znode path to the the node that will be created if an operation fails on any node
   * @param controller controller running the operation
   * @param opInstanceName name of the running operation instance (not the operation description).
   * @return full znode path to the abort znode
   */
  public static String getAbortNode(ZKTwoPhaseCommitController controller, String opInstanceName) {
    return ZKUtil.joinZNode(controller.abortZnode, opInstanceName);
  }
  
  public ZooKeeperWatcher getWatcher() {
    return watcher;
  }

  // --------------------------------------------------------------------------
  // internal debugging methods
  // --------------------------------------------------------------------------
  /**
   * Recursively print the current state of ZK (non-transactional)
   * @param root name of the root directory in zk to print
   * @throws KeeperException
   */
  protected void logZKTree(String root) {
    if (!LOG.isDebugEnabled()) return;
    LOG.debug("Current zk system:");
    String prefix = "|-";
    LOG.debug(prefix + root);
    try {
      logZKTree(root, prefix);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to print the current state of the ZK tree.
   * @see #logZKTree(String)
   * @throws KeeperException if an unexpected exception occurs
   */
  protected void logZKTree(String root, String prefix) throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(watcher, root);
    if (children == null) return;
    for (String child : children) {
      LOG.debug(prefix + child);
      String node = ZKUtil.joinZNode(root.equals("/") ? "" : root, child);
      logZKTree(node, prefix + "---");
    }
  }
}