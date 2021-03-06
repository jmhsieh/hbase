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

package org.apache.hadoop.hbase.regionserver;

/**
 * Factory to create MetricsRegionServerSource when given a  MetricsRegionServerWrapper
 */
public class MetricsRegionServerSourceFactoryImpl implements MetricsRegionServerSourceFactory {
  private static enum FactoryStorage {
    INSTANCE;
    private MetricsRegionServerSource serverSource;
    private MetricsRegionAggregateSourceImpl aggImpl;
  }

  private synchronized MetricsRegionAggregateSourceImpl getAggregate() {
    if (FactoryStorage.INSTANCE.aggImpl == null) {
      FactoryStorage.INSTANCE.aggImpl = new MetricsRegionAggregateSourceImpl();
    }
    return FactoryStorage.INSTANCE.aggImpl;
  }


  @Override
  public synchronized MetricsRegionServerSource createServer(MetricsRegionServerWrapper regionServerWrapper) {
    if (FactoryStorage.INSTANCE.serverSource == null) {
      FactoryStorage.INSTANCE.serverSource = new MetricsRegionServerSourceImpl(
          regionServerWrapper);
    }
    return FactoryStorage.INSTANCE.serverSource;
  }

  @Override
  public MetricsRegionSource createRegion(MetricsRegionWrapper wrapper) {
    return new MetricsRegionSourceImpl(wrapper, getAggregate());
  }
}
