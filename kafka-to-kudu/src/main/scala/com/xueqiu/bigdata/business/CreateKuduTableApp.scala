package com.xueqiu.bigdata.business

import java.io.FileInputStream
import java.sql.DriverManager
import java.util
import java.util.LinkedList

import org.apache.commons.dbutils.DbUtils
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.log4j.Logger
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON


/**
  * 运行参数：
      local:
        "{
            \"kudu.servers\":\"k8s-master01:7051\",
            \"kudu.replicas\": 1,
            \"kudu.buckets\": 3,
            \"kudu.columnFilePath\": \"C:/sync_project/realtime-data-platform/kafka-to-kudu/conf/table-fields.yaml\",
            \"impala.server\": \"k8s-master01:21050\",
            \"impala.username\": \"root\"
        }"
  */
class CreateKuduTableApp
object CreateKuduTableApp extends App {
  private val logger = Logger.getLogger(classOf[CreateKuduTableApp])
  private val argsMap = JSON.parseFull(args(0)).get.asInstanceOf[Map[String, Object]]
  private val kuduServer = argsMap.get("kudu.servers").get.asInstanceOf[String]
  private val tableReplicas = argsMap.get("kudu.replicas").get.asInstanceOf[Double].toInt
  private val tableBuckets = argsMap.get("kudu.buckets").get.asInstanceOf[Double].toInt
  private val columnFilePath =  argsMap.get("kudu.columnFilePath").get.asInstanceOf[String]
  private val impalaServer =  argsMap.get("impala.server").get.asInstanceOf[String]
  private val impalaUsername = argsMap.get("impala.username").get.asInstanceOf[String]

  private val outerList: util.List[util.Map[String, Object]] = new Yaml().loadAs(new FileInputStream(columnFilePath), classOf[util.List[util.Map[String, Object]]])


  def createKuduTable(){
    def newColumn(name: String, tType: Type, isKey: Boolean):ColumnSchema = {
      val column = new ColumnSchema.ColumnSchemaBuilder(name, tType)
      column.key(isKey)
      column.build
    }
    def getKuduType(cType:String): Type = {
      cType match {
        case "INT64" => Type.INT64
        case "INT32" => Type.INT32
        case "INT16" => Type.INT16
        case "INT8" => Type.INT8
        case "DOUBLE" => Type.DOUBLE
        case "STRING" => Type.STRING
        case "DECIMAL" => Type.DECIMAL
        case "BOOL" => Type.BOOL
        case _ => Type.STRING
      }
    }
    val client = new KuduClient.KuduClientBuilder(kuduServer).defaultSocketReadTimeoutMs(6000).build
    try {
      outerList
        .asScala.foreach(elMap1 => {
        val database = elMap1.get("database").asInstanceOf[String]
        elMap1.get("table")
          .asInstanceOf[util.List[util.Map[String, Object]]]
          .asScala
          .foreach(elMap2 =>{
            val columnSchemaList = new LinkedList[ColumnSchema]()
            var primaryKeyColumn:String = null
            val table = elMap2.get("name").asInstanceOf[String]
            elMap2.get("column")
              .asInstanceOf[java.util.List[java.util.Map[String, Object]]]
              .asScala
              .foreach(elMap3 => {
                val isKey = elMap3.get("isPrimaryKey").asInstanceOf[Boolean]
                val cName = elMap3.get("cName").asInstanceOf[String]
                val cType = elMap3.get("cType").asInstanceOf[String]
                if(isKey){
                  primaryKeyColumn = cName
                }
                columnSchemaList.add(newColumn(cName, getKuduType(cType), isKey))
              })
            val schema = new Schema(columnSchemaList)
            val options = new CreateTableOptions()
            options.setNumReplicas(tableReplicas)
            options.addHashPartitions(List(primaryKeyColumn).asJava, tableBuckets)
            if(!client.tableExists(s"${database}.${table}")){
              client.createTable(s"${database}.${table}", schema, options)
              logger.info(s"创建kudu表${database}.${table}成功")
            }
          })
      })
    } finally {
      client.close()
    }
  }

  def createImpalaTable(): Unit ={
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection(s"jdbc:hive2://${impalaServer}/;auth=noSasl", impalaUsername, null)
    val stm = conn.createStatement()
    try {
      outerList
        .asScala.foreach(elMap1 => {
        val database = elMap1.get("database").asInstanceOf[String]
        stm.execute(s"create database if not exists ${database}")
        elMap1.get("table")
          .asInstanceOf[util.List[util.Map[String, Object]]]
          .asScala
          .foreach(elMap2 =>{
            val table = elMap2.get("name").asInstanceOf[String]
            stm.execute(s"CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} STORED AS KUDU TBLPROPERTIES('kudu.table_name' = '${database}.${table}', 'kudu.master_addresses' = '${kuduServer.replace("k8s-master01", "kudu")}')")
          })
      })
      logger.info("创建impala外部kudu表成功")
    } finally {
      DbUtils.close(stm)
      DbUtils.close(conn)
    }
  }

  createKuduTable()
  createImpalaTable()
}
