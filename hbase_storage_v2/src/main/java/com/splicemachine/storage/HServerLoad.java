/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage;

import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.storage.PartitionServerLoad;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.Size;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Scott Fines
 * Date: 1/7/16
 */
public class HServerLoad implements PartitionServerLoad {
  private ServerMetrics metrics;
  private long readRequestsCount = 0;
  private long writeRequestsCount = 0;
  private Set<PartitionLoad> loads;

  public HServerLoad(ServerMetrics metrics) {
    this.metrics = metrics;
    this.loads = new HashSet<>(metrics.getRegionMetrics().size(), 0.9f);
    for (RegionMetrics rl : metrics.getRegionMetrics().values()) {
      readRequestsCount += rl.getReadRequestCount();
      writeRequestsCount += rl.getWriteRequestCount();
      PartitionLoad pl = new HPartitionLoad(rl.getNameAsString(), (int) rl.getStoreFileSize().get(Size.Unit.MEGABYTE), (int) rl.getMemStoreSize().get(Size.Unit.MEGABYTE), (int) rl.getStoreFileIndexSize().get(Size.Unit.MEGABYTE));
      loads.add(pl);
    }
  }

  @Override
  public int numPartitions() {
    return metrics.getRegionMetrics().size();
  }

  @Override
  public long totalWriteRequests() {
    return writeRequestsCount;
  }

  @Override
  public long totalReadRequests() {
    return readRequestsCount;
  }

  @Override
  public long totalRequests() {
    return metrics.getRequestCount();

  }

  @Override
  public Set<PartitionLoad> getPartitionLoads() {
    return loads;
  }
}
