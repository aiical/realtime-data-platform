package com.xueqiu.bigdata

import java.sql.{Connection, DriverManager, Statement}

import org.apache.commons.dbutils.DbUtils
import org.junit.{Before, Test}

class ImpalaTest {
  var conn: Connection = _
  var stm:Statement = _

  @Before
  def before(): Unit ={
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    conn = DriverManager.getConnection("jdbc:hive2://k8s-master01:21050/;auth=noSasl")
    stm = conn.createStatement()
  }


  @Test
  def testCreateDatabase(): Unit ={
    val sql = "create database animal"
    stm.execute(sql)
  }

  @Test
  def testCreateTable(): Unit ={
    val sql = "create EXTERNAL table if not exists animal.dog STORED AS KUDU TBLPROPERTIES('kudu.table_name' = 'animal.dog', 'kudu.master_addresses' = 'kudu:7051')"
//      val sql = "CREATE EXTERNAL TABLE IF NOT EXISTS animal.cat STORED AS KUDU TBLPROPERTIES('kudu.table_name' = 'animal.cat', 'kudu.master_addresses' = 'k8s-master01:7051')"
//    val sql = """
//                |CREATE EXTERNAL TABLE animal.dog STORED AS KUDU
//                |TBLPROPERTIES(
//                |    'kudu.table_name' = 'animal.dog',
//                |    'kudu.master_addresses' = 'kudu:7051')
//              """.stripMargin
    stm.execute(sql)
  }

  @Test
  def testDropTable(): Unit ={
    val sql = "drop table animal.dog"
    stm.execute(sql)
  }

  @Test
  def testDropDatabase(): Unit ={
    val sql = "drop database animal"
    stm.execute(sql)
  }

  def  after(): Unit ={
    DbUtils.close(stm)
    DbUtils.close(conn)
  }
}
