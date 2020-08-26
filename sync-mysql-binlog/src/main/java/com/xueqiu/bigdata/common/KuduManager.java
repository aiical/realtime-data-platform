package com.xueqiu.bigdata.common;

import com.xueqiu.bigdata.entry.KuduColumnBean;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class KuduManager {
    private KuduClient client;


    public void createConnection(String kuduServers, long timeout){
        client = new KuduClient.KuduClientBuilder(kuduServers).defaultSocketReadTimeoutMs(timeout).build();
    }

    public boolean tableExists(String dbTable) throws KuduException {
        return client.tableExists(dbTable);
    }

    public void createTable(String dbTable, List<KuduColumnBean> kuduColumnList, int tableReplicas, int tableBuckets) throws KuduException {
        List<ColumnSchema> columnSchemaList = new ArrayList<>();
        kuduColumnList.forEach(kuduColumnBean -> columnSchemaList.add(newColumn(kuduColumnBean.getcName(), kuduColumnBean.getcType(), kuduColumnBean.isKey())));
        List<String> keyList = kuduColumnList
                .stream()
                .filter(kuduColumnBean -> kuduColumnBean.isKey())
                .map(kuduColumnBean -> kuduColumnBean.getcName())
                .collect(Collectors.toList());
        CreateTableOptions options = new CreateTableOptions()
                .setNumReplicas(tableReplicas)
                .addHashPartitions(keyList, tableBuckets);
        client.createTable(dbTable, new Schema(columnSchemaList), options);
    }

    public void closeConnection() throws KuduException {
        client.close();
    }

    private ColumnSchema newColumn(String cName, Type cType, boolean isKey) {
        return new ColumnSchema.ColumnSchemaBuilder(cName, cType).key(isKey).build();
    }
}
