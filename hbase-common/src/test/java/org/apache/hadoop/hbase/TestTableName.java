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
package org.apache.hadoop.hbase;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

public class TestTableName {

  /**
   * This test verifies that there are not race conditions on instantiation of
   * TableNames due to the tablename cache introduced in HBASE-9976.
   */
  @Test
  public void testTableNameCacheRace() throws InterruptedException, ExecutionException {
    class TableNameConstructor implements Callable<Void> {
      public Void call() {
        for (int i = 0; i < 10000; i++) {
          TableName.valueOf("test");
        }
        return null;
      }
    }

    int size = 1000;
    ExecutorService exe = Executors.newFixedThreadPool(10);
    Future<?>[] fs = new Future<?>[size];
    for (int i = 0; i < size; i++) {
      fs[i] = exe.submit(new TableNameConstructor());
    }

    // this fails if any of these throw an ExecutionException or
    // InterruptedException.
    for (int i = 0; i < size; i++) {
      fs[i].get();
    }
  }
}
