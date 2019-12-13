package org.apache.flink.streaming.connectors.gcp.bigquery;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

public class BigQueryTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {

    private String            datasetName;
    private String            tableName;
    private String[]          fieldNames;
    private TypeInformation[] fieldTypes;

    public BigQueryTableSink(String datasetName, String tableName) {
        this.datasetName = datasetName;
        this.tableName = tableName;
    }

    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    // Streaming
    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        dataStream
            .addSink(new BigQuerySinkFunction(datasetName, tableName, fieldNames))
            .name(TableConnectorUtils.generateRuntimeName(this.getClass(), fieldNames));
    }

    // Batch
    @Override
    public void emitDataSet(DataSet<Row> dataSet) {
        dataSet
            .output(new BigQueryOutputFormat(datasetName, tableName, fieldNames))
            .name(TableConnectorUtils.generateRuntimeName(this.getClass(), fieldNames));
    }
}
