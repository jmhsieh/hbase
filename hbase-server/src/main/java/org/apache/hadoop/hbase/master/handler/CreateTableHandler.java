/**
 *
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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;

/**
 * Handler to create a table.
 */
@InterfaceAudience.Private
public class CreateTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(CreateTableHandler.class);
  protected final MasterFileSystem fileSystemManager;
  protected final HTableDescriptor hTableDescriptor;
  protected final Configuration conf;
  private final AssignmentManager assignmentManager;
  private final CatalogTracker catalogTracker;
  private final HRegionInfo [] newRegions;

  public CreateTableHandler(Server server, MasterFileSystem fileSystemManager,
      HTableDescriptor hTableDescriptor, Configuration conf, HRegionInfo [] newRegions,
      CatalogTracker catalogTracker, AssignmentManager assignmentManager)
          throws NotAllMetaRegionsOnlineException, TableExistsException, IOException {
    super(server, EventType.C_M_CREATE_TABLE);

    this.fileSystemManager = fileSystemManager;
    this.hTableDescriptor = hTableDescriptor;
    this.conf = conf;
    this.newRegions = newRegions;
    this.catalogTracker = catalogTracker;
    this.assignmentManager = assignmentManager;

    int timeout = conf.getInt("hbase.client.catalog.timeout", 10000);
    // Need META availability to create a table
    try {
      if(catalogTracker.waitForMeta(timeout) == null) {
        throw new NotAllMetaRegionsOnlineException();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for meta availability", e);
      InterruptedIOException ie = new InterruptedIOException(e.getMessage());
      ie.initCause(e);
      throw ie;
    }

    String tableName = this.hTableDescriptor.getNameAsString();
    if (MetaReader.tableExists(catalogTracker, tableName)) {
      throw new TableExistsException(tableName);
    }

    // If we have multiple client threads trying to create the table at the
    // same time, given the async nature of the operation, the table
    // could be in a state where .META. table hasn't been updated yet in
    // the process() function.
    // Use enabling state to tell if there is already a request for the same
    // table in progress. This will introduce a new zookeeper call. Given
    // createTable isn't a frequent operation, that should be ok.
    try {
      if (!this.assignmentManager.getZKTable().checkAndSetEnablingTable(tableName))
        throw new TableExistsException(tableName);
    } catch (KeeperException e) {
      throw new IOException("Unable to ensure that the table will be" +
        " enabling because of a ZooKeeper issue", e);
    }
  }


  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" +
      this.hTableDescriptor.getNameAsString();
  }

  @Override
  public void process() {
    String tableName = this.hTableDescriptor.getNameAsString();
    try {
      LOG.info("Attempting to create the table " + tableName);
      MasterCoprocessorHost cpHost = ((HMaster) this.server).getCoprocessorHost();
      if (cpHost != null) {
        cpHost.preCreateTableHandler(this.hTableDescriptor, this.newRegions);
      }
      handleCreateTable(tableName);
      if (cpHost != null) {
        cpHost.postCreateTableHandler(this.hTableDescriptor, this.newRegions);
      }
    } catch (IOException e) {
      LOG.error("Error trying to create the table " + tableName, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to create the table " + tableName, e);
    }
  }

  /**
   * Responsible of table creation (on-disk and META) and assignment.
   * - Create the table directory and descriptor (temp folder)
   * - Create the on-disk regions (temp folder)
   *   [If something fails here: we've just some trash in temp]
   * - Move the table from temp to the root directory
   *   [If something fails here: we've the table in place but some of the rows required
   *    present in META. (hbck needed)]
   * - Add regions to META
   *   [If something fails here: we don't have regions assigned: table disabled]
   * - Assign regions to Region Servers
   *   [If something fails here: we still have the table in disabled state]
   * - Update ZooKeeper with the enabled state
   */
  private void handleCreateTable(String tableName) throws IOException, KeeperException {
    Path tempdir = fileSystemManager.getTempDir();
    FileSystem fs = fileSystemManager.getFileSystem();

    // 1. Create Table Descriptor
    FSTableDescriptors.createTableDescriptor(fs, tempdir, this.hTableDescriptor);
    Path tempTableDir = new Path(tempdir, tableName);
    Path tableDir = new Path(fileSystemManager.getRootDir(), tableName);

    // 2. Create Regions
    List<HRegionInfo> regionInfos = handleCreateHdfsRegions(tempdir, tableName);

    // 3. Move Table temp directory to the hbase root location
    if (!fs.rename(tempTableDir, tableDir)) {
      throw new IOException("Unable to move table from temp=" + tempTableDir +
        " to hbase root=" + tableDir);
    }

    if (regionInfos != null && regionInfos.size() > 0) {
      // 4. Add regions to META
      MetaEditor.addRegionsToMeta(this.catalogTracker, regionInfos);

      // 5. Trigger immediate assignment of the regions in round-robin fashion
      try {
        assignmentManager.getRegionStates().createRegionStates(regionInfos);
        assignmentManager.assign(regionInfos);
      } catch (InterruptedException e) {
        LOG.error("Caught " + e + " during round-robin assignment");
        InterruptedIOException ie = new InterruptedIOException(e.getMessage());
        ie.initCause(e);
        throw ie;
      }
    }

    // 6. Set table enabled flag up in zk.
    try {
      assignmentManager.getZKTable().setEnabledTable(tableName);
    } catch (KeeperException e) {
      throw new IOException("Unable to ensure that the table will be" +
        " enabled because of a ZooKeeper issue", e);
    }
  }

  /**
   * Create the on-disk structure for the table, and returns the regions info.
   * @param tableRootDir directory where the table is being created
   * @param tableName name of the table under construction
   * @return the list of regions created
   */
  protected List<HRegionInfo> handleCreateHdfsRegions(final Path tableRootDir, final String tableName)
      throws IOException {
    int regionNumber = newRegions.length;
    ThreadPoolExecutor regionOpenAndInitThreadPool = getRegionOpenAndInitThreadPool(
        "RegionOpenAndInitThread-" + tableName, regionNumber);
    CompletionService<HRegion> completionService = new ExecutorCompletionService<HRegion>(
        regionOpenAndInitThreadPool);

    List<HRegionInfo> regionInfos = new ArrayList<HRegionInfo>();
    for (final HRegionInfo newRegion : newRegions) {
      completionService.submit(new Callable<HRegion>() {
        public HRegion call() throws IOException {

          // 1. Create HRegion
          HRegion region = HRegion.createHRegion(newRegion,
              tableRootDir, conf, hTableDescriptor, null,
              false, true);
          // 2. Close the new region to flush to disk. Close log file too.
          region.close();
          return region;
        }
      });
    }
    try {
      // 3. wait for all regions to finish creation
      for (int i = 0; i < regionNumber; i++) {
        Future<HRegion> future = completionService.take();
        HRegion region = future.get();
        regionInfos.add(region.getRegionInfo());
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      regionOpenAndInitThreadPool.shutdownNow();
    }

    return regionInfos;
  }

  protected ThreadPoolExecutor getRegionOpenAndInitThreadPool(
      final String threadNamePrefix, int regionNumber) {
    int maxThreads = Math.min(regionNumber, conf.getInt(
        "hbase.hregion.open.and.init.threads.max", 10));
    ThreadPoolExecutor openAndInitializeThreadPool = Threads
    .getBoundedCachedThreadPool(maxThreads, 30L, TimeUnit.SECONDS,
        new ThreadFactory() {
          private int count = 1;

          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, threadNamePrefix + "-" + count++);
            return t;
          }
        });
    return openAndInitializeThreadPool;
  }
}
