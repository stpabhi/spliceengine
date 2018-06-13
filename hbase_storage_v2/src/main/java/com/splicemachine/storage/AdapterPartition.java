/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.storage;

import com.google.protobuf.Service;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.storage.Attributable;
import com.splicemachine.storage.DataPut;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionDescriptor;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.storage.util.PartitionInRangePredicate;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
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
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.ImmutableList;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Iterators;

import javax.annotation.concurrent.NotThreadSafe;
import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

/**
 * Represents an HBase Table as a single Partition.
 *
 * @author Scott Fines
 *         Date: 12/17/15
 */
@NotThreadSafe
public class AdapterPartition extends SkeletonHBaseClientPartition{
    protected static final Logger LOG = Logger.getLogger(AdapterPartition.class);
    // If we ever have an ACL violation, we must use the proxy for all remaining calls
    private volatile static boolean useProxy = false;

    private final DataSource connectionPool;
    private TableName tableName;
    private final Connection connection;
    private PartitionInfoCache partitionInfoCache;
    private ClientPartition delegate;

    public AdapterPartition(ClientPartition clientPartition,
                            Connection connection,
                            DataSource connectionPool,
                            TableName tableName,
                            PartitionInfoCache partitionInfoCache){
        assert tableName!=null:"Passed in tableName is null";
        this.delegate = clientPartition;
        this.tableName=tableName;
        this.connectionPool=connectionPool;
        this.connection=connection;
        this.partitionInfoCache = partitionInfoCache;
    }

    @Override
    public String getTableName(){
        return delegate.getTableName();
    }

    @Override
    public void close() throws IOException{
        delegate.close();
    }

    @Override
    protected Result doGet(Get get) throws IOException{
        if (!useProxy) {
            try {
                return delegate.doGet(get);
            } catch (AccessDeniedException ade) {
                LOG.info("Received ACL violation, activating proxy: " + ade.getMessage());
                useProxy = true;
            }
        }

        try {
            try (java.sql.Connection jdbcConnection = connectionPool.getConnection();
                 PreparedStatement statement = jdbcConnection.prepareStatement("call SYSCS_UTIL.SYSCS_HBASE_OPERATION(?, ?, ?)")) {
                statement.setString(1, tableName.toString());
                statement.setString(2, "get");
                ClientProtos.Get getRequest = ProtobufUtil.toGet(get);
                statement.setBlob(3, new ArrayInputStream(getRequest.toByteArray()));
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for get");
                    }

                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());

                    ClientProtos.Result result = ClientProtos.Result.parseFrom(bytes);
                    return ProtobufUtil.toResult(result);
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected ResultScanner getScanner(Scan scan) throws IOException{
        if (!useProxy) {
            try {
                return delegate.getScanner(scan);
            } catch (AccessDeniedException ade) {
                LOG.info("Received ACL violation, activating proxy: " + ade.getMessage());
                useProxy = true;
            }
        }

        try {
            try (java.sql.Connection jdbcConnection = connectionPool.getConnection();
                 PreparedStatement statement = jdbcConnection.prepareStatement("call SYSCS_UTIL.SYSCS_HBASE_OPERATION(?, ?, ?)")) {
                statement.setString(1, tableName.toString());
                statement.setString(2, "scan");
                Scan copy = new Scan(scan);
                copy.setSmall(true);
                ClientProtos.Scan scanRequest = ProtobufUtil.toScan(copy);
                statement.setBlob(3, new ArrayInputStream(scanRequest.toByteArray()));
                try (ResultSet rs = statement.executeQuery()) {
                    Queue<Result> results = new ArrayDeque<>();
                    while (rs.next()) {
                        Blob blob = rs.getBlob(1);
                        byte[] bytes = blob.getBytes(1, (int) blob.length());

                        ClientProtos.Result result = ClientProtos.Result.parseFrom(bytes);
                        results.add(ProtobufUtil.toResult(result));
                    }
                    return new ResultScanner() {
                        @Override
                        public Result next() throws IOException {
                            return results.poll();
                        }

                        @Override
                        public Result[] next(int nbRows) throws IOException {
                            int size = Math.min(nbRows, results.size());
                            List<Result> r = new ArrayList<>(size);
                            while(size-- > 0) {
                                r.add(results.poll());
                            }
                            return r.toArray(new Result[size]);
                        }

                        @Override
                        public void close() {
                            // nothing
                        }

                        @Override
                        public boolean renewLease() {
                            return false;
                        }

                        @Override
                        public ScanMetrics getScanMetrics() {
                            return null;
                        }

                        @Override
                        public Iterator<Result> iterator() {
                            return results.iterator();
                        }
                    };
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void doDelete(Delete delete) throws IOException{
        throw new UnsupportedOperationException("Delete not supported");
    }

    @Override
    protected void doPut(Put put) throws IOException{
        throw new UnsupportedOperationException("Put not supported");
    }

    @Override
    protected void doPut(List<Put> puts) throws IOException{
        throw new UnsupportedOperationException("Put not supported");
    }

    @Override
    protected long doIncrement(Increment incr) throws IOException{
        throw new UnsupportedOperationException("Increment not supported");
    }

    @Override
    public Iterator<DataResult> batchGet(Attributable attributes,List<byte[]> rowKeys) throws IOException{
        if (!useProxy) {
            try {
                return delegate.batchGet(attributes, rowKeys);
            } catch (AccessDeniedException ade) {
                LOG.info("Received ACL violation, activating proxy: " + ade.getMessage());
                useProxy = true;
            }
        }

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for(byte[] rowKey : rowKeys){
            Get g=new Get(rowKey);
            if(attributes!=null){
                for(Map.Entry<String, byte[]> attr : attributes.allAttributes().entrySet()){
                    g.setAttribute(attr.getKey(),attr.getValue());
                }
            }
            ProtobufUtil.toGet(g).writeDelimitedTo(os);
        }
        List<Result> results = new ArrayList<>();

        try {
            try (java.sql.Connection jdbcConnection = connectionPool.getConnection();
                 PreparedStatement statement = jdbcConnection.prepareStatement("call SYSCS_UTIL.SYSCS_HBASE_OPERATION(?, ?, ?)")) {
                statement.setString(1, tableName.toString());
                statement.setString(2, "batchGet");
                statement.setBlob(3, new ArrayInputStream(os.toByteArray()));
                try (ResultSet rs = statement.executeQuery()) {
                    while(rs.next()) {
                        Blob blob = rs.getBlob(1);
                        byte[] bytes = blob.getBytes(1, (int) blob.length());

                        ClientProtos.Result result = ClientProtos.Result.parseFrom(bytes);
                        results.add(ProtobufUtil.toResult(result));
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            os.close();
        }
        if(results.size()<=0) return Collections.emptyIterator();
        final HResult retResult=new HResult();
        return Iterators.transform(results.iterator(),new Function<Result, DataResult>(){
            @Override
            public DataResult apply(Result input){
                retResult.set(input);
                return retResult;
            }
        });
    }

    @Override
    public boolean checkAndPut(byte[] key,byte[] family,byte[] qualifier,byte[] expectedValue,DataPut put) throws IOException{
        throw new UnsupportedOperationException("Check&put not supported");
    }

    @Override
    public List<Partition> subPartitions(){
        return subPartitions(false);
    }

    public List<Partition> subPartitions(boolean refresh) {
        try {
            List<Partition> partitions;
            if (!refresh) {
                partitions = partitionInfoCache.getAdapterIfPresent(tableName);
                if (partitions == null) {
                    partitions = formatPartitions(getAllRegionLocations(false));
                    assert partitions!=null:"partitions are null";
                    partitionInfoCache.putAdapter(tableName, partitions);
                }
                return partitions;
            }
            partitions = formatPartitions(getAllRegionLocations(true));
            partitionInfoCache.invalidateAdapter(tableName);
            assert partitions!=null:"partitions are null";
            partitionInfoCache.putAdapter(tableName,partitions);
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
        return delegate.getLoad();
    }

    /**
     * Major compacts the table. Synchronous operation.
     * @throws IOException
     */
    @Override
    public void compact(boolean isMajor) throws IOException{
        throw new UnsupportedOperationException("Compact not supported");
    }
    /**
     * Flush a table. Synchronous operation.
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException("Flush not supported");
    }

    public <T extends Service,V> Map<byte[],V> coprocessorExec(Class<T> serviceClass,Batch.Call<T,V> call) throws Throwable{
        return delegate.coprocessorExec(serviceClass,call);
    }

    public Table unwrapDelegate(){
        return delegate.unwrapDelegate();
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
        if (!useProxy) {
            try {
                return delegate.getDescriptor();
            } catch (AccessDeniedException ade) {
                LOG.info("Received ACL violation, activating proxy: " + ade.getMessage());
                useProxy = true;
            }
        }

        try {
            try (java.sql.Connection jdbcConnection = connectionPool.getConnection();
                 PreparedStatement statement = jdbcConnection.prepareStatement("call SYSCS_UTIL.SYSCS_HBASE_OPERATION(?, ?, ?)")) {
                statement.setString(1, tableName.toString());
                statement.setString(2, "descriptor");
                statement.setNull(3, Types.BLOB);
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for descriptor");
                    }

                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());

                    HBaseProtos.TableSchema result = HBaseProtos.TableSchema.parseFrom(bytes);
                    return new HPartitionDescriptor(ProtobufUtil.toTableDescriptor(result));
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

}
