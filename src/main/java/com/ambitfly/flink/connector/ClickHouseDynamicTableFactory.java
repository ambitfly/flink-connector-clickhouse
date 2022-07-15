package com.ambitfly.flink.connector;


import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import java.util.HashSet;
import java.util.Set;

public class ClickHouseDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "clickhouse";

    private static final String DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc database url.");

    public static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc table name.");

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc user name.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc password.");

    public static final ConfigOption<String> FORMAT = ConfigOptions
            .key("format")
            .stringType()
            .noDefaultValue()
            .withDescription("the format.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        requiredOptions.add(FORMAT);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        // either implement your custom validation logic here ...
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig config = helper.getOptions();

        // validate all options
        helper.validate();


        //JdbcReadOptions jdbcReadOptions =
        JdbcConnectionOptions jdbcOptions = getJdbcConnectionOptions(config);
        JdbcDmlOptions jdbcDmlOptions = getJdbcDmlOptions(config);

        // get table schema
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        // table source
        return new ClickHouseDynamicTableSource(jdbcOptions,jdbcDmlOptions, physicalSchema);

    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {

        // either implement your custom validation logic here ...
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);

        final ReadableConfig config = helper.getOptions();

        // validate all options
        helper.validate();

        // get the validated options
        JdbcConnectionOptions jdbcConnectionOptions = getJdbcConnectionOptions(config);
        JdbcDmlOptions jdbcDmlOptions = getJdbcDmlOptions(config);

        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType dataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        // table sink
        return new ClickHouseDynamicTableSink(jdbcConnectionOptions,jdbcDmlOptions, encodingFormat, dataType);
    }

    private JdbcConnectionOptions getJdbcConnectionOptions(ReadableConfig readableConfig) {


        final String url = readableConfig.get(URL);

        final JdbcConnectionOptions.JdbcConnectionOptionsBuilder builder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(DRIVER_NAME)
                .withUrl(url)
                .withUsername( readableConfig.getOptional(USERNAME).get())
                .withPassword(readableConfig.getOptional(PASSWORD).get());
                //.setTableName(readableConfig.get(TABLE_NAME))
                //.setDialect(new ClickHouseDialect());

        //readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        //readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }


    private JdbcDmlOptions getJdbcDmlOptions(ReadableConfig readableConfig) {


        final String url = readableConfig.get(URL);

        String[] filed = new String[1];
        filed[0] = "default";
        final JdbcDmlOptions.JdbcDmlOptionsBuilder builder = new JdbcDmlOptions.JdbcDmlOptionsBuilder()
                .withTableName(readableConfig.get(TABLE_NAME))
                .withDialect(new ClickHouseDialect())
                .withFieldNames(filed);

        //readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        //readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

}