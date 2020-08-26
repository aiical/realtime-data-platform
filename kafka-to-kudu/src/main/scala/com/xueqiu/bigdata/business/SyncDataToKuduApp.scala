package com.xueqiu.bigdata.business

import com.xueqiu.bigdata.business.core.kuduSink
import com.xueqiu.bigdata.common.FlinkBaseProgram
import org.apache.flink.streaming.api.scala._

import scala.util.parsing.json.JSON


/**
  * 运行参数：
      local:
        --common.topic.consumerType earliest
        --kafka.bootstrap.servers k8s-master01:32092
        --kafka.consumer.groupId kafka2Kudu
        --kafka.consumer.topic maxwell
        --kudu.servers k8s-master01:7051
        --zookeeper.servers k8s-master01:32184
        --impala.server k8s-master01:21050
        --impala.username "root"
  */
object SyncDataToKuduApp extends FlinkBaseProgram{
  override def execute(): Unit = {
    val mysqlDmlTypeList = List("bootstrap-insert", "insert", "update", "delete")
    val flinkKafkaConsumer = getFlinkKafkaConsumer(
      conf.getString("kafka.bootstrap.servers", null),
      conf.getString("kafka.consumer.groupId", null),
      conf.getString("kafka.consumer.topic", null)
    )

    sEnv.addSource(flinkKafkaConsumer)
      .map(JSON.parseFull(_))
      .filter(_ != None)
      .map(_.get.asInstanceOf[Map[String, Any]])
      .filter(el => {
        mysqlDmlTypeList.contains(el.getOrElse("type", null)) &&
        el.getOrElse("table", null).asInstanceOf[String] != "bootstrap"
      })
      .addSink(new kuduSink(conf))

    sEnv.execute(jobName)
  }
}
