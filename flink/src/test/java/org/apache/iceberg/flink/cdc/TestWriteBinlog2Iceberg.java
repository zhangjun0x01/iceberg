package org.apache.iceberg.flink.cdc;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public class TestWriteBinlog2Iceberg {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
//    RowData rowData =
//        GenericRowData.ofKind(RowKind.INSERT, 1, org.apache.flink.table.data.StringData.fromString("AAA"));
    env.enableCheckpointing(10000);
    StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

    String sql =
        "CREATE TABLE mysql_binlog (\n"
            + " id int ,\n"
            + " data STRING\n"
            + ") WITH (\n"
            + " 'connector' = 'mysql-cdc',\n"
            + " 'hostname' = 'localhost',\n"
            + " 'port' = '3306',\n"
            + " 'username' = 'root',\n"
            + " 'password' = 'root',\n"
            + " 'database-name' = 'test',\n"
            + " 'table-name' = 'cdc_test'\n"
            + ")";
    tenv.executeSql(sql);

    org.apache.flink.table.api.Table t = tenv.sqlQuery("select * from mysql_binlog");
    DataStream<RowData> dataStream = tenv.toRetractStream(t, RowData.class).map(
        (MapFunction<Tuple2<Boolean, RowData>, RowData>) value -> value.f1);

    dataStream.print();

//    RowData rowData1 =
//        GenericRowData.ofKind(RowKind.INSERT, 1, org.apache.flink.table.data.StringData.fromString("aaaa"));
//    DataStream<RowData> dataStream = env.fromElements( rowData1);

    TableIdentifier identifier = TableIdentifier.of("iceberg_db", "iceberg_cdc_05");
    Configuration configuration = new Configuration();
    String uri = "thrift://10.160.85.186:9083";
    Map map = new HashMap<>();
    map.put(CatalogProperties.URI, uri);
    String catalogName = "iceberg";
    CatalogLoader catalogLoader = CatalogLoader.hive(catalogName, configuration, map);
    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);
    tableLoader.open();
    Table table = tableLoader.loadTable();

    List<String> equalityFieldColumns = Lists.newArrayList("id");

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .equalityFieldColumns(equalityFieldColumns)
        .build();

    env.execute();
  }
}
