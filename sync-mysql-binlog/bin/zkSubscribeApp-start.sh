#!/bin/bash

cur_env=$1

base_dir=$(cd `dirname $0`/../; pwd)
log_dir=${base_dir}/logs
mkdir ${log_dir}
cd ${base_dir}

if [[ cur_env == "product" ]]
then
   zookeeper_servers=
   metrics_port=
else
   zookeeper_servers=k8s-master01:32184
   metrics_port=8080
fi



java -cp sync-mysql-binlog.jar \
    com.xueqiu.bigdata.ZkSubscribeApp \
    ${zookeeper_servers} \
    ${log_dir} \
    conf/maxwell-sync.yaml \
    ${metrics_port}