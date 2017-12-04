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
package com.splicemachine.spark.splicemachine

import java.security.PrivilegedExceptionAction
import java.util.Properties

import com.splicemachine.EngineDriver
import com.splicemachine.access.api.SConfiguration
import com.splicemachine.access.hbase.HBaseConnectionFactory
import com.splicemachine.client.SpliceClient
import com.splicemachine.db.impl.jdbc.EmbedConnection
import com.splicemachine.derby.impl.SpliceSpark
import com.splicemachine.derby.stream.spark.SparkUtils
import com.splicemachine.derby.vti.SpliceDatasetVTI
import com.splicemachine.si.impl.driver.SIDriver
import com.splicemachine.tools.EmbedConnectionMaker
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SerializableWritable
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.reflect.ClassTag

class SplicemachineContext() extends Serializable with Logging {
  @transient var credentials = SparkHadoopUtil.get.getCurrentUserCredentials()
  @transient var appliedCredentials = false
  @transient val job = Job.getInstance()
  TableMapReduceUtil.initCredentials(job)
  val credentialsConf = SpliceSpark.getContextUnsafe.broadcast(new SerializableWritable(job.getCredentials))

  val principal = System.getProperty("spark.yarn.principal")
  val keytab = System.getProperty("spark.yarn.keytab")
  System.err.println("principal " + principal)
  System.err.println("keytab " + keytab)

  val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
  UserGroupInformation.setLoginUser(ugi);

  ugi.doAs(new PrivilegedExceptionAction[Unit]() {
    def run = {
      SpliceClient.isClient = true
      SpliceSpark.setupSpliceStaticComponents()
    }
  })

  val url = "jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin"
  JdbcDialects.registerDialect(new SplicemachineDialect)

  def applyCreds[T]() {
    credentials = SparkHadoopUtil.get.getCurrentUserCredentials()

    logInfo("appliedCredentials:" + appliedCredentials + ",credentials:" + credentials)

    if (!appliedCredentials && credentials != null) {
      appliedCredentials = true
      logCredInformation(credentials)

      @transient val ugi = UserGroupInformation.getCurrentUser
      ugi.addCredentials(credentials)
      // specify that this is a proxy user
      ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)

      ugi.addCredentials(credentialsConf.value.value)
    }
  }

  def logCredInformation[T] (credentials2:Credentials) {
    logInfo("credentials:" + credentials2);
    for (a <- 0 until credentials2.getAllSecretKeys.size()) {
      logInfo("getAllSecretKeys:" + a + ":" + credentials2.getAllSecretKeys.get(a));
    }
    val it = credentials2.getAllTokens.iterator();
    while (it.hasNext) {
      logInfo("getAllTokens:" + it.next());
    }
  }

  private[spark]
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  @transient lazy val internalConnection = {
    System.err.println("Splice Client in SplicemachineContext "+SpliceClient.isClient)
    val engineDriver: EngineDriver = EngineDriver.driver
    assert(engineDriver != null, "Not booted yet!")
    // Create a static statement context to enable nested connections
    val maker: EmbedConnectionMaker = new EmbedConnectionMaker
    val dbProperties: Properties = new Properties
    dbProperties.put("useSpark", "true")
    dbProperties.put("skipConflictDetection", "true")
    dbProperties.put("skipSampling", "true")
    dbProperties.put("insertMode", "INSERT")
    maker.createNew(dbProperties);
    dbProperties.put(EmbedConnection.INTERNAL_CONNECTION, "true")
    maker.createNew(dbProperties)
  }

  def tableExists(schemaTableName: String): Boolean = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      JdbcUtils.tableExists(conn, jdbcOptions.url, jdbcOptions.table)
    } finally {
      conn.close()
    }
  }

  def tableExists(schemaName: String, tableName: String): Boolean = {
    tableExists(schemaName + "." + tableName)
  }

  def dropTable(schemaName: String, tableName: String): Unit = {
    dropTable(schemaName + "." + tableName)
  }


  def dropTable(schemaTableName: String): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      JdbcUtils.dropTable(conn, jdbcOptions.table)
    } finally {
      conn.close()
    }
  }

  def createTable(tableName: String,
                  structType: StructType,
                  keys: Seq[String],
                  createTableOptions: String): Unit = {
    val spliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> tableName)
    val jdbcOptions = new JDBCOptions(spliceOptions)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      val schemaString = JdbcUtils.schemaString(structType, jdbcOptions.url)
      val keyArray = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
      val primaryKeyString = new StringBuilder()
      val dialect = JdbcDialects.get(jdbcOptions.url)
      keyArray foreach { field =>
        val name = dialect.quoteIdentifier(field)
        primaryKeyString.append(s", $name")
      }
      val sql = s"CREATE TABLE $tableName ($schemaString) $primaryKeyString"
      val statement = conn.createStatement
      println(sql)
      statement.executeUpdate(sql)
    } finally {
      conn.close()
    }
  }

  def df(sql: String): Dataset[Row] = {
    SparkUtils.resultSetToDF(internalConnection.createStatement().executeQuery(sql));
  }

  def rdd(schemaTableName: String,
          columnProjection: Seq[String] = Nil): RDD[Row] = {
    val columnList = SpliceJDBCUtil.listColumns(columnProjection.toArray)
    val sqlText = s"SELECT $columnList FROM ${schemaTableName}"
    df(sqlText).rdd
  }

  def insert(dataFrame: DataFrame, schemaTableName: String): Unit = {
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") --splice-properties useSpark=true, skipConflictDetection=true, skipSampling=true, insertMode=INSERT\n select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  def delete(dataFrame: DataFrame, schemaTableName: String): Unit = {
    val jdbcOptions = new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "delete from " + schemaTableName + " where exists (select 1 from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SDVTI (" + schemaString + ") where "
    val dialect = JdbcDialects.get(url)
    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) + " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
    val combinedText = sqlText + whereClause + ")"
    internalConnection.createStatement().executeUpdate(combinedText)
  }

  def update(dataFrame: DataFrame, schemaTableName: String): Unit = {
    val jdbcOptions = new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName))
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val keys = SpliceJDBCUtil.retrievePrimaryKeys(jdbcOptions)
    val prunedFields = dataFrame.schema.fieldNames.filter((p: String) => keys.indexOf(p) == -1)
    val columnList = SpliceJDBCUtil.listColumns(prunedFields)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    val sqlText = "update " + schemaTableName + " " +
      "set (" + columnList + ") = (" +
      "select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SDVTI (" + schemaString + ") where "
    val dialect = JdbcDialects.get(url)
    val whereClause = keys.map(x => schemaTableName + "." + dialect.quoteIdentifier(x) + " = SDVTI." ++ dialect.quoteIdentifier(x)).mkString(" AND ")
    val combinedText = sqlText + whereClause + ")"
    internalConnection.createStatement().executeUpdate(combinedText)
  }


  def bulkImportHFile(dataFrame: DataFrame, schemaTableName: String,
                      options: scala.collection.mutable.Map[String, String]): Unit = {

    val bulkImportDirectory = options.get("bulkImportDirectory")
    if (bulkImportDirectory == null) {
      throw new IllegalArgumentException("bulkImportDirectory cannot be null")
    }
    SpliceDatasetVTI.datasetThreadLocal.set(dataFrame)
    val columnList = SpliceJDBCUtil.listColumns(dataFrame.schema.fieldNames)
    val schemaString = SpliceJDBCUtil.schemaWithoutNullableString(dataFrame.schema, url)
    var properties = "--SPLICE-PROPERTIES "
    options foreach (option => properties += option._1 + "=" + option._2 + ",")
    properties = properties.substring(0, properties.length - 1)

    val sqlText = "insert into " + schemaTableName + " (" + columnList + ") " + properties + "\n" +
      "select " + columnList + " from " +
      "new com.splicemachine.derby.vti.SpliceDatasetVTI() " +
      "as SpliceDatasetVTI (" + schemaString + ")"
    internalConnection.createStatement().executeUpdate(sqlText)
  }

  def getSchema(schemaTableName: String): StructType = {
    val newSpliceOptions = Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> schemaTableName)
    JDBCRDD.resolveTable(new JDBCOptions(newSpliceOptions))
  }

  /**
    * Prune all but the specified columns from the specified Catalyst schema.
    *
    * @param schema  - The Catalyst schema of the master table
    * @param columns - The list of desired columns
    * @return A Catalyst schema corresponding to columns in the given order.
    */
  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.metadata.getString("name") -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }


}