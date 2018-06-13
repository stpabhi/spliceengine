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

import com.google.protobuf.Service;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.impl.HNotServingRegion;
import com.splicemachine.si.impl.HRegionTooBusy;
import com.splicemachine.si.impl.HWrongRegion;
import com.splicemachine.storage.util.PartitionInRangePredicate;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.ImmutableList;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Iterators;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Represents an HBase Table as a single Partition.
 *
 * @author Scott Fines
 *         Date: 12/17/15
 */
@NotThreadSafe
public class ClientPartition extends SkeletonHBaseClientPartition{
    protected static final Logger LOG = Logger.getLogger(ClientPartition.class);
    private TableName tableName;
    private final Table table;
    private final Connection connection;
    private final Clock clock;
    private PartitionInfoCache partitionInfoCache;

    public ClientPartition(Connection connection,
                           TableName tableName,
                           Table table,
                           Clock clock,
                           PartitionInfoCache partitionInfoCache){
        assert tableName!=null:"Passed in tableName is null";
        this.tableName=tableName;
        this.table=table;
        this.connection=connection;
        this.clock = clock;
        this.partitionInfoCache = partitionInfoCache;
    }

    @Override
    public String getTableName(){
        return table.getName().getQualifierAsString();
    }

    @Override
    public void close() throws IOException{
        table.close();
    }

    @Override
    protected Result doGet(Get get) throws IOException{
        return table.get(get);
    }

    @Override
    protected ResultScanner getScanner(Scan scan) throws IOException{
        return table.getScanner(scan);
    }

    @Override
    protected void doDelete(Delete delete) throws IOException{
        table.delete(delete);
    }

    @Override
    protected void doPut(Put put) throws IOException{
        table.put(put);
    }

    @Override
    protected void doPut(List<Put> puts) throws IOException{
        table.put(puts);
    }

    @Override
    protected long doIncrement(Increment incr) throws IOException{
        return Bytes.toLong(table.increment(incr).value());
    }

    @Override
    public Iterator<DataResult> batchGet(Attributable attributes,List<byte[]> rowKeys) throws IOException{
        List<Get> gets=new ArrayList<>(rowKeys.size());
        for(byte[] rowKey : rowKeys){
            Get g=new Get(rowKey);
            if(attributes!=null){
                for(Map.Entry<String, byte[]> attr : attributes.allAttributes().entrySet()){
                    g.setAttribute(attr.getKey(),attr.getValue());
                }
            }
            gets.add(g);
        }
        Result[] results=table.get(gets);
        if(results.length<=0) return Collections.emptyIterator();
        final HResult retResult=new HResult();
        return Iterators.transform(Iterators.forArray(results),new Function<Result, DataResult>(){
            @Override
            public DataResult apply(Result input){
                retResult.set(input);
                return retResult;
            }
        });
    }

    @Override
    public boolean checkAndPut(byte[] key,byte[] family,byte[] qualifier,byte[] expectedValue,DataPut put) throws IOException{
        return table.checkAndPut(key,family,qualifier,expectedValue,((HPut)put).unwrapDelegate());
    }

    @Override
    public List<Partition> subPartitions(){
        return subPartitions(false);
    }

    public List<Partition> subPartitions(boolean refresh) {
        try {
            List<Partition> partitions;
            if (!refresh) {
                partitions = partitionInfoCache.getIfPresent(tableName);
                if (partitions == null) {
                    partitions = formatPartitions(getAllRegionLocations(false));
                    assert partitions!=null:"partitions are null";
                    partitionInfoCache.put(tableName, partitions);
                }
                return partitions;
            }
            partitions = formatPartitions(getAllRegionLocations(true));
            partitionInfoCache.invalidate(tableName);
            assert partitions!=null:"partitions are null";
            partitionInfoCache.put(tableName,partitions);
            return partitions;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Partition> formatPartitions(List<HRegionLocation> tableLocations) {
        List<Partition> partitions=new ArrayList<>(tableLocations.size());
        for(HRegionLocation location : tableLocations){
            RegionInfo regionInfo=location.getRegion();
            partitions.add(new RangedClientPartition(this,regionInfo,new RLServer(location)));
        }
        return partitions;
    }

    @Override
    public PartitionServer owningServer(){
        throw new UnsupportedOperationException("A Table is not owned by a single server, but by the cluster as a whole");
    }
    @Override
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow) {
        return subPartitions(startRow,stopRow,false);
    }

    @Override
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow, boolean refresh) {
        return ImmutableList.copyOf(Iterables.filter(subPartitions(refresh),new PartitionInRangePredicate(startRow,stopRow)));
    }

    @Override
    public PartitionLoad getLoad() throws IOException{
        Map<ServerName, List<RegionInfo>> serverToRegionMap=new HashMap<>();
        try(RegionLocator rl=connection.getRegionLocator(tableName)){
            List<HRegionLocation> locations=rl.getAllRegionLocations();
            for(HRegionLocation location : locations){
                List<RegionInfo> info=serverToRegionMap.get(location.getServerName());
                if(info==null){
                    info = new LinkedList<>();
                    serverToRegionMap.put(location.getServerName(),info);
                }
                info.add(location.getRegion());
            }
        }
        int totalStoreFileSizeMB = 0;
        int totalMemstoreSieMB = 0;
        int storefileIndexSizeMB = 0;
        try(Admin admin=connection.getAdmin()){
            ClusterMetrics clusterMetrics=admin.getClusterMetrics();
            for(Map.Entry<ServerName,List<RegionInfo>> entry:serverToRegionMap.entrySet()){
                ServerMetrics load=clusterMetrics.getLiveServerMetrics().get(entry.getKey());
                Map<byte[], RegionMetrics> regionsLoad=load.getRegionMetrics();
                for(RegionInfo info:entry.getValue()){
                    RegionMetrics rl = regionsLoad.get(info.getRegionName());
                    totalStoreFileSizeMB+=rl.getStoreFileSize().get(Size.Unit.MEGABYTE);
                    totalMemstoreSieMB+=rl.getMemStoreSize().get(Size.Unit.MEGABYTE);
                    storefileIndexSizeMB+=rl.getStoreFileIndexSize().get(Size.Unit.MEGABYTE);
                }
            }
        }
        return new HPartitionLoad(getName(),totalStoreFileSizeMB,totalMemstoreSieMB,storefileIndexSizeMB);
    }

    /**
     * Major compacts the table. Synchronous operation.
     * @throws IOException
     */
    @Override
    public void compact(boolean isMajor) throws IOException{
        HExceptionFactory exceptionFactory = HExceptionFactory.INSTANCE;
        try(Admin admin = connection.getAdmin()){
            if (isMajor)
                admin.majorCompact(tableName);
            else
                admin.compact(tableName);
            CompactionState compactionState=null;
            do{
                try{
                    clock.sleep(500l,TimeUnit.MILLISECONDS);
                }catch(InterruptedException e){
                    throw new InterruptedIOException();
                }

                try {
                    compactionState = admin.getCompactionState(tableName);
                }catch(Exception e){
                    // Catch and ignore the typical region errors while checking compaction state.
                    // Otherwise the client compaction request will fail which we don't want.
                    IOException ioe = exceptionFactory.processRemoteException(e);
                    if (ioe instanceof HNotServingRegion ||
                        ioe instanceof HWrongRegion ||
                        ioe instanceof HRegionTooBusy) {
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG,
                                "Can not fetch compaction state for table %s but we will keep trying: %s.",
                                tableName.getQualifierAsString(), ioe);
                        }
                    } else {
                        throw e;
                    }
                }
            }while(compactionState!=CompactionState.NONE);
        }
    }
    /**
     * Flush a table. Synchronous operation.
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        try(Admin admin = connection.getAdmin()) {
            admin.flush(tableName);
        }
    }

    public <T extends Service,V> Map<byte[],V> coprocessorExec(Class<T> serviceClass,Batch.Call<T,V> call) throws Throwable{
        return table.coprocessorService(serviceClass,getStartKey(),getEndKey(),call);
    }

    public Table unwrapDelegate(){
        return table;
    }


    private List<HRegionLocation> getAllRegionLocations(boolean refresh) throws IOException {
        if (refresh)
           ((ClusterConnection) connection).clearRegionCache(tableName);
        try(RegionLocator regionLocator=connection.getRegionLocator(tableName)){
            return regionLocator.getAllRegionLocations();
        }
    }

    @Override
    public BitSet getBloomInMemoryCheck(boolean hasConstraintChecker,Pair<KVPair, Lock>[] dataAndLocks) throws IOException {
        return null;
    }

    @Override
    public PartitionDescriptor getDescriptor() throws IOException {
        return new HPartitionDescriptor(table.getDescriptor());
    }
}
