package com.splicemachine.test;

import com.splicemachine.hbase.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class SpliceTestCluster {
  private static final Logger LOG = Logger.getLogger(SpliceTestCluster.class);

  protected static HBaseTestingUtility TEST_UTIL;

  private static MiniHBaseCluster CLUSTER;


  public static void setUpDFS() throws Exception {
        TEST_UTIL.getConfiguration().set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        TEST_UTIL.startMiniDFSCluster(1);
        //LOG.info("Started DFS cluster "+TEST_UTIL.getDFSCluster().getFileSystem().listFiles(new Path("/"), true).next().toString());
    }

  public static void setupZoo() throws Exception {
    TEST_UTIL.startMiniZKCluster(1, 2181);
    LOG.info("Started ZooKeeper on "+TEST_UTIL.getZooKeeperWatcher().getQuorum());
  }

  public static void setUpHBase() throws Exception {
//    TEST_UTIL.getConfiguration().set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//    TEST_UTIL.getConfiguration().set("hbase.zookeeper.quorum", "127.0.0.1:2181");
//    TEST_UTIL.getConfiguration().setInt("hbase.master.port", 60000);
//    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 60010);
//    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.port", 60020);
//    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.info.port", 60030);
//    if (System.getProperty("os.name").contains("Mac") ) {
//      String interfaceName = "lo0";
//      TEST_UTIL.getConfiguration().set("hbase.zookeeper.dns.interface", interfaceName);
//      TEST_UTIL.getConfiguration().set("hbase.master.dns.interface", interfaceName);
//      TEST_UTIL.getConfiguration().set("hbase.regionserver.dns.interface", interfaceName);
//    }

//    Path rootdir = TEST_UTIL.getDataTestDirOnTestFS("hbase");
//    FSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootdir);

    CLUSTER = new MiniHBaseCluster(TEST_UTIL.getConfiguration(), 1, 1);
    System.out.println(CLUSTER.getClusterMetrics());
  }

  public static void tearDown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
    CLUSTER.join();
//        if (KDC != null) {
//            KDC.stop();
//        }
    TEST_UTIL.shutdownMiniCluster();
  }


  public static void main(String[] args) throws Exception {
//        if (args.length != 7) {
//            SpliceTestPlatformUsage.usage("Unknown argument(s)", null);
//        }
    try {

      String hbaseRootDirUri = "hbase";
      Integer masterPort = 60000;
      Integer masterInfoPort = 60010;
      Integer regionServerPort = 60020;
      Integer regionServerInfoPort = 60030;
      Integer derbyPort = 1527;
      boolean failTasksRandomly = false;

      Configuration config = SpliceTestPlatformConfig.create(hbaseRootDirUri,
            masterPort,
            masterInfoPort,
            regionServerPort,
            regionServerInfoPort,
            derbyPort,
            failTasksRandomly, null);
      TEST_UTIL = new HBaseTestingUtility(config);

      // KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri().getPath());
      setupZoo();
      setUpDFS();
      // clean-up zookeeper
//      try {
//        ZkUtils.delete("/hbase/master");
//      } catch (KeeperException.NoNodeException ex) {
//        // ignore
//      }
//      try {
//        ZkUtils.recursiveDelete("/hbase/rs");
//      } catch (KeeperException.NoNodeException | IOException ex) {
//        // ignore
//      }

      setUpHBase();
      LOG.info("HBase cluster status "+CLUSTER.getClusterStatus());
      CLUSTER.getMaster().getConnection().getAdmin()
            .createTable(TableDescriptorBuilder
                  .newBuilder(TableName.valueOf("abc"))
                  .addColumnFamily(ColumnFamilyDescriptorBuilder.of("cf1"))
                  .build());

      TableName[] tables = CLUSTER.getMaster().getConnection().getAdmin().listTableNames();
      for (TableName tableName : tables) {
        System.out.println(tableName.getNameAsString());
      }
//      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//        try {
//          tearDown();
//        } catch (Exception e) {
//          e.printStackTrace();
//        }
//      }));
      //  new SpliceTestPlatformShutdownThread(CLUSTER, KDC, TEST_UTIL);
    } catch (NumberFormatException e) {
      SpliceTestPlatformUsage.usage("Bad port specified", e);
    }
  }

}
