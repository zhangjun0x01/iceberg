package org.apache.iceberg.flink.cdc;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public class TestWriteCDC {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    RowData rowData = GenericRowData.ofKind(RowKind.INSERT, 1, StringData.fromString("AAA"));
//    RowData rowData1 = GenericRowData.ofKind(RowKind.INSERT, 1, StringData.fromString("AAA"));

//    RowData rowData1 = GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 1, StringData.fromString("AAA"));
//    RowData rowData2 = GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1, StringData.fromString("BBB"));

    DataStream<RowData> dataStream = env.fromElements(rowData);

    TableIdentifier identifier = TableIdentifier.of("iceberg_db", "iceberg_cdc_test8");
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
