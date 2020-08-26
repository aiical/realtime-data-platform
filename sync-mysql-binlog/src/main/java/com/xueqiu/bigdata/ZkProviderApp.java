package com.xueqiu.bigdata;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.File;

/**
 * @author lvxw
 *运行参数：
    local:
        k8s-master01:32184
        C:/sync_project/realtime-data-platform/sync-mysql-binlog/conf/maxwell-sync.yaml
 */
public class ZkProviderApp {
    private static final String zkNode = "/mysql-binlog-sync";

    public static void main(String[] args) throws Exception {
        String zkServers = args[0];
        String yamlFilePath = args[1];

        CuratorFramework curatorClient = CuratorFrameworkFactory
                .builder()
                .connectString(zkServers)
                .connectionTimeoutMs(30 * 1000)
                .sessionTimeoutMs(60 * 1000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();
        curatorClient.start();

        if(curatorClient.checkExists().forPath(zkNode) == null){
            curatorClient.create().withMode(CreateMode.PERSISTENT).forPath(zkNode, FileUtils.readFileToString(new File(yamlFilePath), "UTF-8").getBytes());
        }
        curatorClient.setData().forPath(zkNode, FileUtils.readFileToString(new File(yamlFilePath), "UTF-8").getBytes());

        curatorClient.close();
    }
}
