package com.ambitfly.flink.connector;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.logical.RowType;

public class ClickHouseDynamicTableSource implements ScanTableSource {

    private final JdbcConnectionOptions options;
    private final JdbcDmlOptions jdbcDmlOptions;
    private final TableSchema tableSchema;

    public ClickHouseDynamicTableSource(JdbcConnectionOptions options,JdbcDmlOptions jdbcDmlOptions, TableSchema tableSchema) {
        this.jdbcDmlOptions = jdbcDmlOptions;
        this.options = options;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        final JdbcDialect dialect = jdbcDmlOptions.getDialect();

        String query = dialect.getSelectFromStatement(
                jdbcDmlOptions.getTableName(), tableSchema.getFieldNames(), new String[0]);

        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        final JdbcRowDataInputFormat.Builder builder = JdbcRowDataInputFormat.builder()
                .setDrivername(options.getDriverName())
                .setDBUrl(options.getDbURL())
                .setUsername(options.getUsername().orElse(null))
                .setPassword(options.getPassword().orElse(null))
                .setQuery(query)
                .setRowConverter(dialect.getRowConverter(rowType))
                .setRowDataTypeInfo(runtimeProviderContext
                        .createTypeInformation(tableSchema.toRowDataType()));

        return InputFormatProvider.of(builder.build());

    }

    @Override
    public DynamicTableSource copy() {
        return new ClickHouseDynamicTableSource(options,jdbcDmlOptions, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse Table Source";
    }

}