package org.apache.flink.streaming.connectors.gcp.bigquery;

import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// FIXME: This has NEVER been run at all.

public class BigQueryStreamTableSinkFactory implements StreamTableSinkFactory<Row> {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("update-mode", "append");
        context.put("connector.type", "bigquery");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> list = new ArrayList<>();
        list.add("connector.debug");
        list.add("dataset");
        list.add("tablename");
        return list;
    }

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        String dataset   = properties.get("dataset");
        String tablename = properties.get("tablename");

        return new BigQueryTableSink(dataset, tablename);
    }
}
