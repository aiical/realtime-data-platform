package com.xueqiu.bigdata.business.core

import java.math.BigDecimal
import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util.concurrent.TimeUnit

import com.xueqiu.bigdata.util.DistributeLock
import org.apache.commons.dbutils.DbUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kudu.Type
import org.apache.kudu.client.{AlterTableOptions, KuduClient, SessionConfiguration}
import org.apache.log4j.Logger

class kuduSink(conf: Configuration) extends RichSinkFunction[Map[String,Any]]{
  private var logger:Logger = _
  private var client:KuduClient = _

  override def open(parameters: Configuration): Unit = {
    client = new KuduClient.KuduClientBuilder(conf.getString("kudu.servers", null)).build
    logger = Logger.getLogger(classOf[kuduSink])

    // 订阅// 生成database、table、field 、type 、 pk 并保存到zk节点 变更事件， 更新hashMap(不要clear)

  }

  override def invoke(outerMap: Map[String, Any], context: SinkFunction.Context[_]): Unit = {
    val dmlType = outerMap.get("type").get.asInstanceOf[String].toLowerCase
    var lock:DistributeLock = null

    if(dmlType == "table-alter") {
      try {
        // 加分布式锁
        lock = new DistributeLock("/kafka-to-kudu", conf.getString("zookeeper.servers", null))
        lock.acquire(30L, TimeUnit.SECONDS)

        // 生成database、table、field 、type 、 pk 并保存到zk节点
        //先跟新新hashMap, 再跟新zk数据

        KuduDdl(outerMap)
        impalaRefresh(outerMap, conf)
      } finally {
        // 释放分布式锁
        if (lock != null) {
          lock.release()
        }
      }
    }else{
      KuduDml(outerMap)
    }
  }

  override def close(): Unit = {
    client.close();
  }

  private def KuduDml(outerMap:Map[String, Any]): Unit ={
    val database = outerMap.get("database").get
    val table = outerMap.get("table").get
    val dmlType = outerMap.get("type").get
    val dataMap = outerMap.get("data").get.asInstanceOf[Map[String, Any]]

    val kuduTable = client.openTable(s"${database}.${table}")
    val operation = dmlType match {
      case "insert" | "bootstrap-insert" =>  kuduTable.newInsert()
      case "delete" =>  kuduTable.newDelete()
      case "update" =>  kuduTable.newUpdate()
    }
    val session = client.newSession
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC)
    val row = operation.getRow

    dataMap.foreach(kv => {
      val pk = kv._1
      val pkValue = kv._2
      val pkValueType = kv._2.getClass.getSimpleName
      pkValueType match {
        case "Long" => row.addLong(pk, pkValue.asInstanceOf[Long])
        case "Int" => row.addInt(pk, pkValue.asInstanceOf[Int])
        case "Short" => row.addShort(pk, pkValue.asInstanceOf[Short])
        case "String" => row.addString(pk, pkValue.asInstanceOf[String])
        case "Double" => row.addDouble(pk, pkValue.asInstanceOf[Double])
        case "Float" => row.addFloat(pk, pkValue.asInstanceOf[Float])
        case "BigDecimal" => row.addDecimal(pk, pkValue.asInstanceOf[BigDecimal])
        case "Boolean" => row.addBoolean(pk, pkValue.asInstanceOf[Boolean])
        case "Byte" => row.addByte(pk, pkValue.asInstanceOf[Byte])
        case "Array" => row.addBinary(pk, pkValue.asInstanceOf[Array[Byte]])
      }

    })
    session.flush()
    session.apply(operation)
    session.close()
  }

  private def KuduDdl(outerMap:Map[String, Any]): Unit ={
    val database = outerMap.get("database").asInstanceOf[String]
    val table = outerMap.get("table").asInstanceOf[String]
    val alterArr = outerMap.get("sql").asInstanceOf[String].split(",")

    def alterForDrop(): Unit ={
      alterArr.filter(_.startsWith("DROP")).map(str => {
        val cName = str.split("`")(1)
        val options = new AlterTableOptions().dropColumn(cName)
        client.alterTable(s"${database}.${table}", options)
      })
    }
    def alterForChange(): Unit ={
      alterArr.filter(_.startsWith("CHANGE ")).map(str =>{
        val arr = str.split("`")
        val oldName = arr(1)
        val newName = arr(3)
        val options =  new AlterTableOptions().renameColumn(oldName, newName)
        client.alterTable(s"${database}.${table}", options)
      })
    }
    def alterForAdd(): Unit ={
      val kuduMysqlTypeMap =  Map(
        "CHAR" -> Type.STRING,
        "VARCHAR" -> Type.STRING,
        "FLOAT" -> Type.FLOAT,
        "DOUBLE" -> Type.DOUBLE
      )
      alterArr.filter(_.startsWith("ADD ")).map(str => {
        val arr = str.split("`")
        val cName = arr(1)
        val cType = kuduMysqlTypeMap.getOrElse(arr(2).split("\\(")(0).trim, Type.STRING)
        val options = new AlterTableOptions().addNullableColumn(cName, cType)
        client.alterTable(s"${database}.${table}", options)
      })
    }

    alterForDrop()
    alterForChange()
    alterForAdd()
  }

  private def impalaRefresh(outerMap:Map[String, Any], conf: Configuration): Unit ={

    val database = outerMap.get("database").asInstanceOf[String]
    val table = outerMap.get("table").asInstanceOf[String]
    val refreshSql = s"refresh ${database}.${table}"

    var conn: Connection = null
    var stm:Statement = null
    try {
      val url =  s"jdbc:hive2://${conf.getString("impala.server", null)}/;auth=noSasl"
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      conn = DriverManager.getConnection(url, conf.getString("impala.username", null), null)
      stm = conn.createStatement()
      stm.execute(refreshSql)
    } catch {
      case e: Exception => throw new RuntimeException(s"执行imapla刷新语句失败${refreshSql}", e)
    } finally{
      try{
        DbUtils.close(stm)
      } catch {
        case e: SQLException => logger.error(e)
      }
      try{
        DbUtils.close(conn)
      } catch {
        case e: SQLException => logger.error(e)
      }
    }
  }
}

