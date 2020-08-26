package com.xueqiu.bigdata;

import com.xueqiu.bigdata.common.ZkManager;
import com.xueqiu.bigdata.core.MysqlBinLogSyncManager;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.io.File;
import java.util.Properties;

/**
 * @author lvxw
 通过zookeeper订阅指定的节点， 当节点数据变化时， 触发操作：
    0) 检测数据内容是否满足调度要求， 如果通过进入后续操作.
    1）停掉所有的maxwll相关进程：
        A. 停掉所有maxwell-bootstrap进程
        B. 停掉所有maxwell-bootstrap、maxwell进程
    2）根据节点新的数据内容：
        A. 解析yaml数据
        B. 启动所有的maxwell-bootstrap
        c. 启动所有的maxwell
 运行参数：
    local:
        k8s-master01:32184
            C:/sync_project/realtime-data-platform/sync-mysql-binlog/logs
C:/sync_project/realtime-data-platform/sync-mysql-binlog/conf/maxwell-sync.yaml
8080
 */
public class ZkSubscribeApp {
    private static final Logger logger = Logger.getLogger(ZkSubscribeApp.class);
    private static final String zkNode = "/mysql-binlog-sync";
    private static  ZkManager zkManager = null;

    public static void checkZkPath(String yamlFilePath) {
        try {
            if(!zkManager.isExists(zkNode)){
                zkManager.createNode(CreateMode.PERSISTENT, zkNode, FileUtils.readFileToString(new File(yamlFilePath), "UTF-8").getBytes());
            }
        } catch (Exception e) {
            logger.error("检查zookeeper节点失败", e);
        }
    }


    public static void subscribeZkNode(String baseLogDir, String metricsPort, Properties props){
        try {
            NodeCache nodeCache=new NodeCache(zkManager.curatorClient, zkNode);
            nodeCache.start();
            nodeCache.getListenable().addListener(() -> {
                logger.info(String.format("zookeeper节点:%s数据发生变化, 即将触发maxwell重启", zkNode));
                String yamlContent = new String(nodeCache.getCurrentData().getData());

                // 生成database、table、field 、type 、 pk 并保存到zk节点

                MysqlBinLogSyncManager manager = new MysqlBinLogSyncManager(yamlContent, baseLogDir, metricsPort, props);
                manager.initByYamlContent();
                manager.stopAllShellProcess();
                manager.startAllShellProcess();
            });
        } catch (Exception e) {
            logger.error("zookeeper监听节点失败", e);
        }
    }


    public static void main(String[] args){
        try {
            String zkServers = args[0];
            String baseLogDir = args[1];
            String yamlFilePath = args[2];
            String metricsPort = args[3];

            ZkManager zkManager = new ZkManager();
            zkManager.createConnection(zkServers, 30, 60, 5);

            checkZkPath(yamlFilePath);
            subscribeZkNode(baseLogDir, metricsPort, null);

            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            logger.error(e);
        }finally {
            zkManager.closeConnection();
        }
    }
}
