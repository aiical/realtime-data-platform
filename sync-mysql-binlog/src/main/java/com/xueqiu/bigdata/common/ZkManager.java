package com.xueqiu.bigdata.common;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class ZkManager {
    public CuratorFramework curatorClient = null;

    public void createConnection(String zkServers, int connTimeout, int sessionTimeout, int maxRetries){
        curatorClient = CuratorFrameworkFactory
                .builder()
                .connectString(zkServers)
                .connectionTimeoutMs(connTimeout)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(new ExponentialBackoffRetry(1000, maxRetries))
                .build();

        curatorClient.start();
    }

    public String createNode(CreateMode createMode, String zkNode,  byte[] data) throws Exception {
        return curatorClient.create().withMode(createMode).forPath(zkNode, data);
    }

    public Stat setData(String zkNode, byte[] data) throws Exception {
       return curatorClient.setData().forPath(zkNode, data);
    }

    public byte[] getData(String zkNode) throws Exception {
        return curatorClient.getData().forPath(zkNode);
    }

    public boolean isExists(String zkNode) throws Exception {
       return  curatorClient.checkExists().forPath(zkNode) != null;
    }

    public void closeConnection(){
        curatorClient.close();
    }
}
