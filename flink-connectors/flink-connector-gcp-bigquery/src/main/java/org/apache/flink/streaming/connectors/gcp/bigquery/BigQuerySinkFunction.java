package org.apache.flink.streaming.connectors.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;

public class BigQuerySinkFunction extends RichSinkFunction<Row> implements CheckpointedFunction {
    private String   datasetName;
    private String   tableName;
    private BigQuery bigquery;
    private TableId  tableId;

    private transient BigQueryOutputFormat outputFormat = null;

    private String[] fieldNames;

    private boolean debug = true;

    private PrintSinkOutputWriter<String> writer;

    public BigQuerySinkFunction(String datasetName, String tableName, String[] fieldNames) {
        this.datasetName = datasetName;
        this.tableName = tableName;
        this.fieldNames = fieldNames;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        outputFormat = new BigQueryOutputFormat(datasetName, tableName, fieldNames);

        StreamingRuntimeContext context = (StreamingRuntimeContext)this.getRuntimeContext();
        outputFormat.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
    }

    @Override
    public void close() throws Exception {
        outputFormat.close();
    }

    @Override
    public void invoke(Row record, Context context) throws Exception {
        outputFormat.writeRecord(record);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }
}
