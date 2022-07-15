package com.ambitfly.flink.connector;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class ClickHouseDynamicTableSink implements DynamicTableSink {

    private final JdbcConnectionOptions jdbcOptions;
    private final JdbcDmlOptions jdbcDmlOptions;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType dataType;

    public ClickHouseDynamicTableSink(JdbcConnectionOptions jdbcOptions,JdbcDmlOptions jdbcDmlOptions, EncodingFormat<SerializationSchema<RowData>> encodingFormat, DataType dataType) {
        this.jdbcDmlOptions = jdbcDmlOptions;
        this.jdbcOptions = jdbcOptions;
        this.encodingFormat = encodingFormat;
        this.dataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        SerializationSchema<RowData> serializationSchema = encodingFormat.createRuntimeEncoder(context, dataType);

        ClickHouseSinkFunction clickHouseSinkFunction = new ClickHouseSinkFunction(jdbcOptions,jdbcDmlOptions, serializationSchema);

        return SinkFunctionProvider.of(clickHouseSinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new ClickHouseDynamicTableSink(jdbcOptions, jdbcDmlOptions,encodingFormat, dataType);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse Table Sink";
    }

}