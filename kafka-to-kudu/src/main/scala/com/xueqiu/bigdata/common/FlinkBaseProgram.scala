package com.xueqiu.bigdata.common

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.log4j.Logger


object FlinkBaseProgram{
  scala.util.Properties.setProp("scala.time", "true")
  implicit def hadoopUser:String = "root"
}

abstract class FlinkBaseProgram extends App {
  @transient lazy protected val logger = Logger.getLogger(this.getClass)
  protected val conf : Configuration = ParameterTool.fromArgs(args).getConfiguration
  lazy protected val bEnv : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  lazy protected val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
  lazy protected  val jobName = this.getClass.getSimpleName.replaceFirst("\\$$","")

  protected def getFlinkKafkaConsumer(bootstrapServers:String, groupId:String, topic:String): FlinkKafkaConsumer[String] = {
    val props = new java.util.Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("group.id", groupId)
    val flinkKafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props)

    val consumerType = conf.getString("common.topic.consumerType",null)
    if(consumerType != null){
      val methodName = s"setStartFrom${consumerType.substring(0,1).toUpperCase()}${consumerType.substring(1).toLowerCase()}"
      if(List("setStartFromEarliest", "setStartFromLatest", "setStartFromCroupOffsets", "setStartFromSpecificOffsets").contains(methodName)){
        flinkKafkaConsumer.getClass.getMethod(methodName).invoke(flinkKafkaConsumer)
      }else{
        throw new RuntimeException(s"Flink does not support this type of consumption: ${consumerType}, you can select one from [earliest, latest, croupOffsets, specificOffsets]")
      }
    }
    flinkKafkaConsumer
  }

  def execute(): Unit

  def init(): Unit ={
    sEnv.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(60, TimeUnit.SECONDS), org.apache.flink.api.common.time.Time.of(60, TimeUnit.SECONDS)))
    sEnv.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
    sEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(5)
    sEnv.getCheckpointConfig.setCheckpointTimeout(1200 * 1000)
    sEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(20)
  }

  init()
  execute()
}
