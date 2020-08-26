#!/bin/bash

cur_env=$1

base_dir=$(cd `dirname $0`/../; pwd)
cd ${base_dir}

if [[ cur_env == "product" ]]
then
   zk_servers=
else
   zk_servers=k8s-master01:32184

fi

java -cp sync-mysql-binlog.jar \
    com.xueqiu.bigdata.ZkProviderApp \
    ${zk_servers} \
    conf/maxwell-sync.yaml