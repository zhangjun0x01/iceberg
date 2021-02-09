package org.apache.iceberg.flink.cdc;

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;

public class CreateV2Table {
  public static void main(String[] args) {

    String tablename = "iceberg_cdc_test10";

    Configuration configuration = new Configuration();
    String uri = "thrift://10.160.85.186:9083";
    Map map = new HashMap<>();
    map.put(CatalogProperties.URI, uri);
    CatalogLoader catalogLoader = CatalogLoader.hive("iceberg", configuration, map);
    HiveCatalog hiveCatalog = (HiveCatalog) catalogLoader.loadCatalog();

    TableIdentifier identifierNew = TableIdentifier.of("iceberg_db", tablename);
    TableOperations ops = hiveCatalog.newTableOps(identifierNew);

    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get())
    );

    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.EQUALITY_FIELD_COLUMNS, "id");

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "hdfs://10.160.85.185/user/hive2/warehouse/iceberg_db.db/" + tablename,
            properties,
            2);
    ops.commit(null, metadata);
  }
}
