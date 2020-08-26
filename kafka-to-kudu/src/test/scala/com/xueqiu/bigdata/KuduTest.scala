package com.xueqiu.bigdata

import java.util.LinkedList

import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.{CreateTableOptions, KuduClient, KuduException, SessionConfiguration}
import org.junit.Test

class KuduTest {
  val masterAddress = "k8s-master01"
  var client = new KuduClient.KuduClientBuilder(masterAddress).defaultSocketReadTimeoutMs(6000).build

  private def newColumn(name: String, tType: Type, isKey: Boolean):ColumnSchema = {
    val column = new ColumnSchema.ColumnSchemaBuilder(name, tType)
    column.key(isKey)
    column.build
  }

  @Test
  def testCreateTable(): Unit ={
    val columns = new LinkedList[ColumnSchema]()
    val columnSchema = newColumn("d_name", Type.STRING, true)
    columns.add(columnSchema)


    val schema = new Schema(columns)
    //创建表时提供的所有选项
    val options = new CreateTableOptions
    // 设置表的replica备份和分区规则
    val parcols = new LinkedList[String]()
    parcols.add("d_name")

    //设置表的备份数
    options.setNumReplicas(1)
    //设置range分区
    options.setRangePartitionColumns(parcols)
    //设置hash分区和数量
    options.addHashPartitions(parcols, 3)

    client.createTable("animal.dog", schema, options)

    client.close()
  }

  @Test
  def testAddColumn(): Unit ={
    val columns = new LinkedList[ColumnSchema]()
    val columnSchema = newColumn("d_age", Type.STRING, true)
    columns.add(columnSchema)


    val schema = new Schema(columns)
    //创建表时提供的所有选项
    val options = new CreateTableOptions
    // 设置表的replica备份和分区规则
    val parcols = new LinkedList[String]()
    parcols.add("d_name")

    //设置表的备份数
    options.setNumReplicas(1)
    //设置range分区
    options.setRangePartitionColumns(parcols)
    //设置hash分区和数量
    options.addHashPartitions(parcols, 3)

    client.createTable("animal.dog", schema, options)

    client.close()
  }

  @Test
  def updateData(): Unit = {
    try {
      val table = client.openTable("animal.dog")
      val session = client.newSession()
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC)
      //更新数据
      val update = table.newUpdate
      val row1 = update.getRow
      row1.addString("d_name", "YYYY")
      session.apply(update)
    } catch {
      case e: KuduException =>
        e.printStackTrace()
    }
  }

  @Test
  def testDeleteTable(): Unit ={
    client.deleteTable("animal.dog")
    client.deleteTable("animal.cat")
    client.deleteTable("animal2.dog")
    client.deleteTable("animal2.cat")
    client.close()
  }
}
