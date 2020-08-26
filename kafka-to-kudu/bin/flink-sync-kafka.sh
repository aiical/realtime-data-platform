#!/bin/bash

cur_env=$1

base_dir=$(cd `dirname $0`/../; pwd)
cd ${base_dir}

if [[ cur_env == "product" ]]
then
   kafka_servers=
   kafka_groupId=
   kafka_topic=
   kudu_servers=
   zookeeper_servers=
   impala_server=
   impala_username=hive
else
   kafka_servers=k8s-master01:32092
   kafka_groupId=kafkaToKudu
   kafka_topic=maxwell
   kudu_servers=k8s-master01:7051
   zookeeper_servers=zookeeper1:2181,zookeeper2:2181,zookeeper3:2181
   impala_server=k8s-master01:21050
   impala_username=root
fi

flink run -m yarn-cluster -ys 8 -ynm myapp -yn 4 -yjm 1024 -ytm 4096 -d \
    -c com.xueqiu.bigdata.business.SyncDataToKuduApp \
    kafka-to-kudu.jar \
    --kafka.bootstrap.servers ${kafka_servers} \
    --kafka.consumer.groupId ${kafka_groupId} \
    --kafka.consumer.topic ${kafka_topic}\
    --kudu.servers ${kudu_servers} \
    --zookeeper.servers ${zookeeper_servers} \
    --impala.server ${impala_server} \
    --impala.username ${impala_username} \
    --impala.password ${impala_password} \
