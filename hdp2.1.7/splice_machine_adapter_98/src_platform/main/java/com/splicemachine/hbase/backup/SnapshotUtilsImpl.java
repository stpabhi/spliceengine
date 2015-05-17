package com.splicemachine.hbase.backup;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;

public class SnapshotUtilsImpl extends SnapshotUtilsBase {
    static final Logger LOG = Logger.getLogger(SnapshotUtilsImpl.class);

    public List<Object> getFilesForFullBackup(String snapshotName, HRegion region) throws IOException {
        Configuration conf = SpliceConstants.config;
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);
        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);

        return getSnapshotFilesForRegion(region ,conf, fs, snapshotDir);
    }

    /**
     * Extract the list of files (HFiles/HLogs) to copy 
     * @return list of files referenced by the snapshot
     */
    public List<Object> getSnapshotFilesForRegion(final HRegion reg, final Configuration conf,
                                                  final FileSystem fs, final Path snapshotDir) throws IOException {
        final String regionName = (reg != null) ? reg.getRegionNameAsString() : null;
        SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

        final List<Object> files = new ArrayList<Object>();
        final String table = snapshotDesc.getTable();

        // Get snapshot files
        SpliceLogUtils.info(LOG,"Loading Snapshot '" + snapshotDesc.getName() + "' hfile list");
        SnapshotReferenceUtil.visitReferencedFiles(fs, snapshotDir,
                new SnapshotReferenceUtil.FileVisitor() {
                    public void storeFile (final String region, final String family, final String hfile)
                            throws IOException {
                        if( regionName == null || isRegionTheSame(regionName, region) ){
                            Path path = HFileLink.createPath(TableName.valueOf(table), region, family, hfile);
                            HFileLink link = new HFileLink(conf, path);
                            if( isReference(hfile) ) {
                                Path p = materializeRefFile(conf, fs, link, reg);
                                files.add(p);
                                SpliceLogUtils.info(LOG, "Add %s to snapshot", p);
                            } else{
                                files.add(link);
                                SpliceLogUtils.info(LOG, "Add %s to snapshot", link);
                            }
                        }
                    }

                    public void recoveredEdits (final String region, final String logfile)
                            throws IOException {
                        // copied with the snapshot referenecs
                    }

                    public void logFile (final String server, final String logfile)
                            throws IOException {
                        //long size = new HLogLink(conf, server, logfile).getFileStatus(fs).getLen();
                        files.add(new Path(server, logfile));
                    }
                });

        return files;
    }
    @Override
    public List<Object> getSnapshotFilesForRegion(final HRegion region, final Configuration conf,
                                                  final FileSystem fs, final String snapshotName) throws IOException {
        Path rootDir = FSUtils.getRootDir(conf);

        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
        List<Object> paths = getSnapshotFilesForRegion(region, conf, fs, snapshotDir);

        return paths;
    }

    public HFileLink getReferredFileLink(HFileLink ref) throws IOException {
        return HFileLink.create(SpliceConstants.config,
                TableName.valueOf(getTableName(ref.getOriginPath()).getBytes()),
                getRegionName(ref.getOriginPath()),
                getColumnFamilyName(ref.getOriginPath()),
                getFileName(ref.getOriginPath()));
    }

    public static HFileLink newLink(Configuration conf,Path linkPath) throws IOException{
        return new HFileLink(conf,linkPath);
    }
}
