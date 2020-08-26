package com.xueqiu.bigdata.core;

import com.xueqiu.bigdata.common.KuduManager;
import com.xueqiu.bigdata.common.ZkManager;
import com.xueqiu.bigdata.common.jdbcManager.AbstractJDBCManager;
import com.xueqiu.bigdata.common.jdbcManager.ImpalaJdbcManager;
import com.xueqiu.bigdata.common.jdbcManager.MysqlJdbcManager;
import com.xueqiu.bigdata.entry.KuduColumnBean;
import com.xueqiu.bigdata.entry.ZkDataBean;
import com.xueqiu.bigdata.util.KuduUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author lvxw
 */
public class MysqlBinLogSyncManager {
    private static boolean hasInitMaxwellDatabase = false;
    private static final Logger logger = Logger.getLogger(MysqlBinLogSyncManager.class);
    private static final String maxwellInitCmdTemplate= "maxwell --user=%s --password=%s --host=%s --port=%s --producer=stdout --filter=exclude:*.*";
    private static final String maxwellBootstrapCmdTemplate = "maxwell-bootstrap --user=%s --password=%s --host=%s --port=%s --database=%s --table=%s --client_id=%s";
    private static final String maxwellCmdTemplate= "maxwell --metrics_type=http  --metrics_jvm=true --http_port=%s --user=%s --password=%s --host=%s --port=%s --producer=kafka --kafka.bootstrap.servers=%s --kafka_topic=%s --client_id=%s --producer_partition_by=table --bootstrapper=sync --output_ddl=true --filter=exclude:*.*";

    private static List<List<Process>> allShellProcessList = new ArrayList<>(2);
    private String yamlContent;
    private String baseLogDir;
    private String metricsPort;
    private Properties props;

    public MysqlBinLogSyncManager(String yamlContent, String baseLogDir, String metricsPort, Properties props){
        this. baseLogDir = baseLogDir;
        this.yamlContent = yamlContent;
        this.metricsPort = metricsPort;
        this.props = props;
    }

    public void initByYamlContent(){
        List list;

        // yaml文件是否为空
        if(yamlContent.trim().isEmpty()){
            throw new RuntimeException("ymal文件为空");
        }

        // 校验yaml格式是否正确
        try{
            list = (List)new Yaml().loadAs(yamlContent, Map.class).get("process");
        }catch (Exception e){
            throw new RuntimeException("yaml格式错误：", e);
        }

        // 检查yaml是否包含 common mysql kafka三个节点
        list.forEach(obj ->{
            Map map = (Map)obj;
            if(map.get("common")==null || map.get("mysql")==null || map.get("kafka") == null){
                throw  new RuntimeException("ymal中必须包含common mysql kafka三个节点");
            }
        });

        // 解析表结构，创建kudu表和impala表， 并将表结构写入zookeeper节点：
        try {
            createKuduAndImpalaTable(list, props);
        } catch (Exception e) {
            throw new RuntimeException("【创建kudu表和impala表， 并将表结构写入zookeeper节点】操作失败", e);
        }


        // 创建kafka Topic
        try {
            list.forEach(obj ->{
                Map map = (Map)obj;
                Map kafka = (Map) map.get("kafka");
                String zkServer = (String) kafka.get("zkServer");
                String topic = (String) kafka.get("topic");
                int partition = (int) kafka.get("partition");
                int duplication = (int) kafka.get("duplication");
                ZkUtils zkUtils = ZkUtils.apply(zkServer, 30000, 30000, JaasUtils.isZkSecurityEnabled());
                if(!AdminUtils.topicExists(zkUtils, topic)){
                    AdminUtils.createTopic(zkUtils, topic, partition, duplication, new Properties(), RackAwareMode.Enforced$.MODULE$);
                    logger.info(String.format("Kafka topic:%s不存在, 自动创建完成", topic));
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("检查Kafka topic失败", e);
        }

        // 创建maxwell库下的所有表
        if(!hasInitMaxwellDatabase){
            logger.info("初始化maxwell库下所有表");
            try {
                for(Object obj: list){
                    Map map = (Map)obj;
                    Map common = (Map) map.get("common");
                    String initShellCmd = String.format(
                            maxwellInitCmdTemplate,
                            common.get("username"),
                            common.get("password"),
                            common.get("host"),
                            common.get("port")
                    );
                    logger.info("initShellCmd:"+ initShellCmd);
                    Process process = Runtime.getRuntime().exec(initShellCmd);
                    process.waitFor(10, TimeUnit.SECONDS);

                    if(process.isAlive()){
                        process.destroyForcibly();
                    }
                }
                hasInitMaxwellDatabase = true;
            } catch (Exception e) {
                throw new RuntimeException("初始化maxwell.bootstrap数据库失败", e);
            }
        }

        logger.info("checkYaml完成");
    }

    public void stopAllShellProcess(){
        allShellProcessList.forEach(processList ->{
            for(int i=processList.size()-1; i>=0; i--){
                Process process = processList.get(i);
                logger.info(String.format("即将停止maxwell第%d号进程", i));
                if(process.isAlive()){
                    process.destroyForcibly();
                }
            }
        });
        allShellProcessList.clear();
        logger.info("dropAllShellProcess完成");
    }

    public void startAllShellProcess(){
        List<List<Map<String, String>>> allPairCmdList= getAllPairCmdListFromYaml(yamlContent);
        allPairCmdList.forEach(pairCmdList ->{
            List<Process> processList = new ArrayList<>();
            for(int i=0; i< pairCmdList.size(); i++){
                Map<String, String> cmdMap = pairCmdList.get(i);
                for(Map.Entry<String, String> entry: cmdMap.entrySet()){
                    String key  =  entry.getKey();
                    String cmd = entry.getValue();
                    int finalI = i;
                    new Thread(() -> {
                        logger.info(finalI +"maxwell启动命令:"+cmd);
                        InputStream inputStream = null;
                        InputStream errorStream = null;
                        try {
                            Process process = Runtime.getRuntime().exec(cmd);
                            processList.add(process);
                            String dateStr = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new Date(System.currentTimeMillis()));
                            inputStream = process.getInputStream();
                            errorStream = process.getErrorStream();
                            FileUtils.copyInputStreamToFile(inputStream, new File(String.format("%s/%s_info_%s.log", baseLogDir, key, dateStr)));
                            FileUtils.copyInputStreamToFile(errorStream, new File(String.format("%s/%s_error_%s.log", baseLogDir, key, dateStr)));
                            process.waitFor();
                        }catch (IOException e){
                            logger.error("关闭输出流失败", e);
                        }catch (Exception e) {
                            logger.error("执行maxwell命令失败", e);
                        }finally {
                            IOUtils.closeQuietly(inputStream);
                            IOUtils.closeQuietly(errorStream);
                        }
                    }).start();
                }
                try {
                    if(i == 0){
                        TimeUnit.SECONDS.sleep(cmdMap.size()*10);
                    }
                } catch (InterruptedException e) {
                    logger.error("countDownLatch等待超时", e);
                }
            }
            allShellProcessList.add(processList);
        });
    }

    private boolean isTableHasBootstrap(String database, String table, String host, int port, String username, String password){
        Connection conn = null;
        Statement stm = null;
        ResultSet resultSet = null;
        String url = String.format("jdbc:mysql://%s:%d/maxwell", host, port, database);
        String querySql = String.format("select count(*) from bootstrap where database_name='%s' and table_name='%s';", database, table);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url,username,password);
            stm = conn.createStatement();
            resultSet = stm.executeQuery(querySql);
            resultSet.next();
            long aLong = resultSet.getLong(1);
            return aLong > 0;
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException(String.format("检查%s.%s是否已引导，失败！", database, table));
        } finally {
            try {
                DbUtils.close(resultSet);
            } catch (SQLException e) {
                logger.error(e);
            }
            try {
                DbUtils.close(stm);
            } catch (SQLException e) {
                logger.error(e);
            }
            try {
                DbUtils.close(conn);
            } catch (SQLException e) {
                logger.error(e);
            }
        }
    }

    private List<List<Map<String, String>>> getAllPairCmdListFromYaml(String yamlContent){
        Map yamlContentMap = new Yaml().loadAs(yamlContent, Map.class);
        List processList = (List)yamlContentMap.get("process");

        List<List<Map<String, String>>> allPairCmdList = new ArrayList<>(2);

        processList.forEach(object ->{
            Map process = (Map) object;
            String collectionName= (String) process.get("name");
            Map commonMap = (Map) process.get("common");
            Map kafkaMap = (Map) process.get("kafka");
            List mysqlList = (List) process.get("mysql");

            ArrayList<Map<String, String>> everyPairCmdMapList = new ArrayList<>(2);
            everyPairCmdMapList.add(getMaxwellBootstrapCmd(commonMap, mysqlList, collectionName));
            everyPairCmdMapList.add(getMaxwellCmd(commonMap, kafkaMap, collectionName, mysqlList, collectionName));
            allPairCmdList.add(everyPairCmdMapList);
        });

        return allPairCmdList;
    }

    private Map<String, String> getMaxwellBootstrapCmd(Map commonMap, List mysqlList, String collectName){
        Map<String, String> intervalCmdMap = new HashMap<>();
        mysqlList.forEach(object ->{
            Map dbTables = (Map)object;
            String  database = (String) dbTables.get("database");
            List<String>  tableList = (List<String>) dbTables.get("tables");
            tableList.forEach(table ->{
                if(!isTableHasBootstrap(database, table, (String) commonMap.get("host"),(int) commonMap.get("port"), (String) commonMap.get("username"), (String) commonMap.get("password"))){
                    String intervalCmd = String.format(
                            maxwellBootstrapCmdTemplate,
                            commonMap.get("username"),
                            commonMap.get("password"),
                            commonMap.get("host"),
                            commonMap.get("port"),
                            database,
                            table,
                            collectName
                    );
                    String key = String.format("%s#%s.%s", collectName, database, table);
                    intervalCmdMap.put(key,intervalCmd);
                }
            });
        });
        return intervalCmdMap;
    }

    private Map<String, String> getMaxwellCmd(Map commonMap, Map kafkaMap, String collectionName, List<Map>  mysqlList, String collectName){
        String maxwellCmdInterval =  String.format(
                maxwellCmdTemplate,
                metricsPort,
                commonMap.get("username"),
                commonMap.get("password"),
                commonMap.get("host"),
                commonMap.get("port"),
                kafkaMap.get("bootstrap"),
                kafkaMap.get("topic"),
                collectionName
        );
        StringBuilder stringBuilder = new StringBuilder(maxwellCmdInterval);
        mysqlList.forEach(map ->{
            String database = (String)map.get("database");
            List<String> tableList = (List<String>) map.get("tables");
            tableList.forEach(table -> stringBuilder.append(String.format(",include:%s.%s", database, table)));
        });

        Map<String, String> maxwellCmdMap = new HashMap<>(1);
        maxwellCmdMap.put(collectionName+"#All", stringBuilder.toString());
        return maxwellCmdMap;
    }

    private void createKuduAndImpalaTable(List outerList, Properties props) throws Exception{
        int tableReplicas = Integer.parseInt(props .getProperty("kudu.tableReplicas"));
        int tableBuckets = Integer.parseInt(props.getProperty("kudu.tableBuckets"));
        String kuduServers =  props.getProperty("kudu.servers");
        KuduManager kuduManager = new KuduManager();
        kuduManager.createConnection(props.getProperty("kudu.servers"), 60000);

        AbstractJDBCManager impalaJdbcManager = new ImpalaJdbcManager();
        impalaJdbcManager.createConnection(String.format("jdbc:hive2://%s/;auth=noSasl", props.get("impala.servers")), props.getProperty("impala.username"), null);

        Map<String, ZkDataBean> zkDataMap = new HashMap<>(64);
        ZkManager zkManager = new ZkManager();
        zkManager.createConnection(
            props.getProperty("zookeeper.servers"),
            Integer.parseInt( props.getProperty("zookeeper.connTimeout")),
            Integer.parseInt( props.getProperty("zookeeper.sessionTimeout")),
            Integer.parseInt( props.getProperty("zookeeper.maxRetries"))
        );

        for(Object obj: outerList){
            Map<String, Object> elMap = (Map<String, Object>) obj;
            Map<String, Object> commonMap  = (Map<String, Object>) elMap.get("common");
            String mysqlHost = (String) commonMap.get("host");
            int mysqlPort = (int)commonMap.get("port");
            String mysqlUsername = (String) commonMap.get("username");
            String mysqlPassword = (String) commonMap.get("password");

            AbstractJDBCManager mysqlJdbcManager = new MysqlJdbcManager();
            mysqlJdbcManager.createConnection(String.format("jdbc:mysql://%s:%d/", mysqlHost, mysqlPort), mysqlUsername, mysqlPassword);

            List<Map<String, Object>> mysqlList = (List<Map<String, Object>>) elMap.get("mysql");
            for(Map<String, Object> elMap2: mysqlList){
                String database = (String) elMap2.get("database");
                List<String> tableList = (List<String>) elMap2.get("tables");

                for(String table: tableList){
                    ResultSet resultSet = mysqlJdbcManager.execueQuery(String.format("desc %s.%s", database, table));
                    List<KuduColumnBean> kuduColumnBeanList = new ArrayList<>();
                    while (resultSet.next()){
                        String cName = resultSet.getNString("Field");
                        String cTypeStr = resultSet.getNString("Type");
                        String isKeyStr = resultSet.getNString("Key");
                        KuduColumnBean kuduColumnBean = new KuduColumnBean(cName, KuduUtils.getKuduType(cTypeStr),isKeyStr == "PRI");
                        kuduColumnBeanList.add(kuduColumnBean);

                        ZkDataBean zkDataBean = new ZkDataBean(cTypeStr, KuduUtils.getKuduType(cTypeStr), isKeyStr == "PRI");
                        zkDataMap.put(String.format("%s.%s.%s", database, table, cName), zkDataBean);
                    }
                    kuduManager.createTable(String.format("%s.%s", database, table), kuduColumnBeanList, tableReplicas, tableBuckets);
                    String impalaCreateSql = String.format(
                            "CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s STORED AS KUDU TBLPROPERTIES('kudu.table_name' = '%s.%s', 'kudu.master_addresses' = '%s')",
                            database,
                            table,
                            database,
                            table,
                            kuduServers.replace("k8s-master01", "kudu")
                    );
                    impalaJdbcManager.execute(impalaCreateSql);
                }
            }
            mysqlJdbcManager.closeConnection();
        }

        String zkNode = "sync-mysql-binlog_table";
        if(!zkManager.isExists(zkNode)){
            zkManager.createNode(CreateMode.PERSISTENT, zkNode, "".getBytes());
        }

        byte[] zkManagerData = zkManager.getData(zkNode);
        if(!new String(zkManagerData).isEmpty()){
            ByteArrayInputStream bais = new ByteArrayInputStream(zkManagerData);
            ObjectInputStream ois = new ObjectInputStream(bais);
            Map<String, ZkDataBean> oldHashMap = (Map<String, ZkDataBean>) ois.readObject();
            zkDataMap.putAll(oldHashMap);
        }


        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(zkDataMap);
        byte[] byteArr = baos.toByteArray();
        oos.flush();
        zkManager.setData(zkNode, byteArr);


        zkManager.closeConnection();
        impalaJdbcManager.closeConnection();
        kuduManager.closeConnection();
    }
}
