### 本项目致力于建设实时数据同步平台。当前重点是实时同步mysql数据到kudu

#### 项目分两大模块

- sync-mysql-binlog： 通过maxwell同步mysql binlog到kafka
- kafka-to-kudu： 通过flink消费kafka topic 实时将数据写入kudu  