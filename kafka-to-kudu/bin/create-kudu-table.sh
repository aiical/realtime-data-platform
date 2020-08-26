#!/bin/bash

cur_env=$1

base_dir=$(cd `dirname $0`/../; pwd)
cd ${base_dir}

if [[ cur_env == "product" ]]
then
   kudu_servers=
   kudu_replicas=
   kudu_buckets=
   impala_server=
   impala_username=hive
else
   kudu_servers=k8s-master01:7051
   kudu_replicas=1
   kudu_buckets=3
   impala_server=k8s-master01:21050
   impala_username=root
fi

java -cp sync-mysql-binlog.jar \
    com.xueqiu.bigdata.ZkProviderApp \
    "{
        \"kudu.servers\":\"${kudu_servers}\",
        \"kudu.replicas\": ${kudu_replicas},
        \"kudu.buckets\": ${kudu_buckets},
        \"kudu.columnFilePath\": \"conf/table-fields.yaml\",
        \"impala.server\": \"${impala_server}\",
        \"impala.username\": \"${impala_username}\",
    }"