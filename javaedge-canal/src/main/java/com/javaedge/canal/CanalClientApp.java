package com.javaedge.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.CaseFormat;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;

public class CanalClientApp {
    public static void main(String[] args) throws Exception {

        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("localhost", 11111),
                "example",
                null, null);

        while (true) {
            connector.connect();
            connector.subscribe("test_db.users");
            Message message = connector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size()>0) {
                for (CanalEntry.Entry entry : entries) {
                    String tableName = entry.getHeader().getTableName();

                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                    CanalEntry.EventType eventType = rowChange.getEventType();

                    if (eventType == CanalEntry.EventType.INSERT) {
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            HashMap<Object, Object> map = new HashMap<>();
                            for (CanalEntry.Column column : afterColumnsList) {
                                String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                                map.put(key, column.getValue());
                            }
                            System.out.println("tableName=" + tableName + "  map=" + JSON.toJSONString(map));
                        }
                    }
                }

            }

        }

    }
}
