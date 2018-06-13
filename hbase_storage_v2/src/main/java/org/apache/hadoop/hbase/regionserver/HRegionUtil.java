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

package org.apache.hadoop.hbase.regionserver;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.io.hfile.HFileUtil;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;

/**
 * Class for accessing protected methods in HBase.
 */
public class HRegionUtil extends BaseHRegionUtil {
  private static final Logger LOG = Logger.getLogger(HRegionUtil.class);

  public static void lockStore(Store store) {
    ((HStore) store).lock.readLock().lock();
  }

  public static void unlockStore(Store store) {
    ((HStore) store).lock.readLock().unlock();
  }

  public static List<byte[]> getCutpoints(HStore store, byte[] start, byte[] end, int requestedSplits) throws IOException {
    assert Bytes.startComparator.compare(start, end) <= 0 || start.length == 0 || end.length == 0;
    if (LOG.isTraceEnabled())
      SpliceLogUtils.trace(LOG, "getCutpoints");
    Collection<HStoreFile> storeFiles;
    storeFiles = store.getStorefiles();
    HFile.Reader fileReader = null;
    List<byte[]> cutPoints = new ArrayList<byte[]>();
    int carry = 0;

    byte[] regionStart = store.getRegionInfo().getStartKey();
    byte[] regionEnd = store.getRegionInfo().getEndKey();

    if (regionStart != null && regionStart.length > 0) {
      if (start == null || Bytes.startComparator.compare(start, regionStart) < 0) {
        start = regionStart;
      }
    }
    if (regionEnd != null && regionEnd.length > 0) {
      if (end == null || Bytes.endComparator.compare(end, regionEnd) > 0) {
        end = regionEnd;
      }
    }

    int splitBlockSize = HConfiguration.getConfiguration().getSplitBlockSize();
    if (requestedSplits > 0) {
      long totalStoreFileInBytes = 0;
      for (HStoreFile file : storeFiles) {
        if (file != null) {
          totalStoreFileInBytes += file.getFileInfo().getFileStatus().getLen();
        }
      }
      long bytesPerSplit = totalStoreFileInBytes / requestedSplits;
      if (bytesPerSplit > Integer.MAX_VALUE) {
        splitBlockSize = Integer.MAX_VALUE;
      } else {
        splitBlockSize = (int) bytesPerSplit;
      }
    }

    Pair<byte[], byte[]> range = new Pair<>(start, end);
    for (HStoreFile file : storeFiles) {
      if (file != null) {
        long storeFileInBytes = file.getFileInfo().getFileStatus().getLen();
        if (LOG.isTraceEnabled())
          SpliceLogUtils.trace(LOG, "getCutpoints with file=%s with size=%d", file.getPath(), storeFileInBytes);
        file.initReader();
        fileReader = file.getReader().getHFileReader();
        carry = HFileUtil.addStoreFileCutpoints(cutPoints, fileReader, storeFileInBytes, carry, range, splitBlockSize);
      }
    }

    if (storeFiles.size() > 1) {  // have to sort, hopefully will not happen a lot if major compaction is working properly...
      Collections.sort(cutPoints, new Comparator<byte[]>() {
        @Override
        public int compare(byte[] left, byte[] right) {
          return org.apache.hadoop.hbase.util.Bytes.compareTo(left, right);
        }
      });
    }

    // add region start key at beginning
    cutPoints.add(0, store.getRegionInfo().getStartKey());
    // add region end key at end
    cutPoints.add(store.getRegionInfo().getEndKey());

    if (LOG.isDebugEnabled()) {
      RegionInfo regionInfo = store.getRegionInfo();
      String startKey = "\"" + CellUtils.toHex(regionInfo.getStartKey()) + "\"";
      String endKey = "\"" + CellUtils.toHex(regionInfo.getEndKey()) + "\"";
      LOG.debug("Cutpoints for " + regionInfo.getRegionNameAsString() + " [" + startKey + "," + endKey + "]: ");
      for (byte[] cutpoint : cutPoints) {
        LOG.debug("\t" + CellUtils.toHex(cutpoint));
      }
    }
    return cutPoints;
  }


  /**
   * The number of bytes stored in each "secondary index" entry in addition to
   * key bytes in the non-root index block format. The first long is the file
   * offset of the deeper-level block the entry points to, and the int that
   * follows is that block's on-disk size without including header.
   */
  static final int SECONDARY_INDEX_ENTRY_OVERHEAD = Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG;

  public static BitSet keyExists(boolean hasConstraintChecker, Store store, Pair<KVPair, Lock>[] dataAndLocks) throws IOException {
    BitSet bitSet = new BitSet(dataAndLocks.length);
    if (!(store instanceof HStore)) {
      return null;
    }

    HStore hstore = (HStore) store;
    hstore.lock.readLock().lock();
    Collection<HStoreFile> storeFiles;
    try {
      storeFiles = hstore.getStorefiles();
      /*
       * Apparently, there's an issue where, when you first start up an HBase instance, if you
       * call this code directly, you can break. In essence, there are no storefiles, so it goes
       * to the memstore, where SOMETHING (and I don't know what) causes it to mistakenly return
       * false,
       * which tells the writing code that it's safe to write, resulting in some missing Primary Key
       * errors.
       *
       * And in practice, it doesn't do you much good to check the memstore if there are no store
       * files,
       * since you'll just have to turn around and check the memstore again when you go to perform
       * your
       * get/scan. So may as well save ourselves the extra effort and skip operation if there are no
       * store
       * files to check.
       */
      if (storeFiles.size() <= 0) return null;
      StoreFileReader fileReader;

      // Check Store Files

      byte[][] keys = new byte[dataAndLocks.length][];
      int[] keyOffset = new int[dataAndLocks.length];
      int[] keyLength = new int[dataAndLocks.length];
      for (int i = 0; i < dataAndLocks.length; i++) {
        if (dataAndLocks[i] == null) continue;
        if (hasConstraintChecker || !KVPair.Type.INSERT.equals(dataAndLocks[i].getFirst().getType())) {
          keys[i] = dataAndLocks[i].getFirst().getRowKey(); // Remove Array Copy (Is this buffered?)...
          keyOffset[i] = 0;
          keyLength[i] = keys[i].length;
        }
      }

      for (HStoreFile file : storeFiles) {
        if (file != null) {
          file.initReader();
          fileReader = file.getReader();
          BloomFilter bloomFilter = fileReader.generalBloomFilter;
          if (bloomFilter == null)
            bitSet.set(0, dataAndLocks.length); // Low level race condition, need to go to scan
          else {
            for (int j = 0; j < keys.length; j++) {
              if (bloomFilter.contains(keys[j], keyOffset[j], keyLength[j],  null))
                bitSet.set(j);
            }
          }
        }
      }
      NavigableSet<Cell> memstore = getKvset(hstore);
      NavigableSet<Cell> snapshot = getSnapshot(hstore);
      for (int i = 0; i < dataAndLocks.length; i++) {
        if (dataAndLocks[i] == null) continue;
        byte[] key = dataAndLocks[i].getFirst().getRowKey();
        if (hasConstraintChecker || !KVPair.Type.INSERT.equals(dataAndLocks[i].getFirst().getType())) {
          if (!bitSet.get(i)) {
            Cell kv = new KeyValue(key,
                  SIConstants.DEFAULT_FAMILY_BYTES,
                  SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                  HConstants.LATEST_TIMESTAMP,
                  HConstants.EMPTY_BYTE_ARRAY);
            bitSet.set(i, checkMemstore(memstore, key, kv) || checkMemstore(snapshot, key, kv));
          }
        }
      }
      return bitSet;
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw ioe;
    } finally {
      hstore.lock.readLock().unlock();
    }
  }

  protected static boolean checkMemstore(NavigableSet<Cell> kvSet, byte[] key, Cell kv) {
    Cell placeHolder;
    try {
      kvSet = kvSet.tailSet(kv, true);
      placeHolder = kvSet.isEmpty() ? null : (Cell) kvSet.first();
      if (placeHolder != null && CellUtil.matchingRows(placeHolder, key))
        return true;
    } catch (NoSuchElementException ignored) {
    } // This keeps us from constantly performing key value comparisons for empty set
    return false;
  }

  public static NavigableSet<Cell> getKvset(HStore store) {
    return ((DefaultMemStore) store.memstore).active.getCellSet();
  }

  public static NavigableSet<Cell> getSnapshot(HStore store) {
    return ((DefaultMemStore) store.memstore).snapshot.getCellSet();
  }


}
