package org.apache.flink.streaming.connectors.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.zetasketch.HyperLogLogPlusPlus;
import com.twitter.chill.Base64;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryOutputFormat extends RichOutputFormat<Row> {
    private String   datasetName;
    private String   tableName;
    private BigQuery bigquery;
    private TableId  tableId;

    // https://cloud.google.com/bigquery/quotas
    // Maximum rows per request: 10,000 rows per request.
    //      We recommend a maximum of 500 rows.
    //      Batching can increase performance and throughput to a point, but at the cost of per-request latency.
    //      Too few rows per request and the overhead of each request can make ingestion inefficient.
    //      Too many rows per request and the throughput may drop.
    static final int  GOOGLE_RECOMMENDED_MAXIMUM_BATCH_SIZE = 500;
//    static final int  GOOGLE_RECOMMENDED_MAXIMUM_BATCH_SIZE = 50; // FIXME: Debugging only
    static final long DEFAULT_FLUSH_INTERVAL_MILLS          = 0;

    private String[] fieldNames;

    private boolean debug = true;

    private PrintSinkOutputWriter<String> writer;

    public BigQueryOutputFormat(String datasetName, String tableName, String[] fieldNames) {
        this.datasetName = datasetName;
        this.tableName = tableName;
        this.fieldNames = fieldNames;
    }

    @Override
    public void configure(Configuration parameters) {

    }

    private InsertAllRequest.Builder insertAllRequestBuilder;
    private long recordsInRequest;

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        writer = new PrintSinkOutputWriter<>("[BIGQUERY]", true);
        writer.open(taskNumber, numTasks);
        if (debug) {
            writer.write("OPEN: " + datasetName + ":" +  tableName);
        }

        tableId = TableId.of(datasetName, tableName);
        bigquery = BigQueryOptions.getDefaultInstance().getService();

        flush();
    }

    @Override
    public void writeRecord(Row record) throws IOException {
        // Values of the row to insert
        Map<String, Object> rowContent = new HashMap<>();

        // For each field in the row
        for (int i = 0 ; i < record.getArity() ; i++) {
            Object fieldValue = record.getField(i);

            // The HyperLogLogPlusPlus MUST be converted into a base64 string before inserting.
            if (fieldValue instanceof HyperLogLogPlusPlus) {
                fieldValue = Base64.encodeBytes(((HyperLogLogPlusPlus)fieldValue).serializeToByteArray());
            }

            rowContent.put(fieldNames[i], fieldValue.toString());
        }

        if (debug) {
            writer.write("WRITE: " + rowContent.toString());
        }

        if (insertAllRequestBuilder == null) {
            insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
            recordsInRequest = 0;
        }

        insertAllRequestBuilder.addRow(rowContent);
        recordsInRequest ++;

        if (recordsInRequest >= GOOGLE_RECOMMENDED_MAXIMUM_BATCH_SIZE) {
            flush();
        }

        // TODO: Do we NEED a rowid ?
        //       String rowId = Base64.encodeBytes(value.toString().getBytes(UTF_8));
//                    .addRow(rowId, rowContent)

    }

    public void flush(){
        if (insertAllRequestBuilder == null || recordsInRequest == 0){
            return; // Nothing to do
        }

        if (debug) {
            writer.write("Flushing  " + recordsInRequest + " records to " +  tableName);
        }

        InsertAllResponse response = bigquery.insertAll(insertAllRequestBuilder.build());

        if (response.hasErrors()) {
            // If any of the insertions failed, this lets you inspect the errors
            writer.write("ERRORS when trying to write " + recordsInRequest + " records.");
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                writer.write("-------- ERROR: " + entry.toString());
            }
        }

        insertAllRequestBuilder = null;
        recordsInRequest = 0;
    }


    @Override
    public void close() {
        flush();
    }
}
