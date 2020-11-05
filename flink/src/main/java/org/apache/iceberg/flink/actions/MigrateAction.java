/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.actions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.Action;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.OrcMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateAction implements Action {
  private static final Logger LOG = LoggerFactory.getLogger(MigrateAction.class);

  private static final PathFilter HIDDEN_PATH_FILTER =
      p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");

  private final HiveCatalog hiveCatalog;
  private final String hiveSourceDbName;
  private final String hiveSourceTableName;
  private final Catalog icebergCatalog;
  private final String icebergDbName;
  private final String icebergTableName;

  public MigrateAction(HiveCatalog hiveCatalog, String hiveSourceDbName, String hiveSourceTableName,
                       Catalog icebergCatalog, String icebergDbName, String icebergTableName) {
    this.hiveCatalog = hiveCatalog;
    this.hiveSourceDbName = hiveSourceDbName;
    this.hiveSourceTableName = hiveSourceTableName;
    this.icebergCatalog = icebergCatalog;
    this.icebergDbName = icebergDbName;
    this.icebergTableName = icebergTableName;
  }

  @Override
  public Object execute() {
    try {
      // hive source table
      ObjectPath tableSource = new ObjectPath(hiveSourceDbName, hiveSourceTableName);

      //获取hive表的相关信息，用于构造iceberg表
      CatalogBaseTable catalogBaseTable = hiveCatalog.getTable(tableSource);
      TableSchema hiveTableSchema = catalogBaseTable.getSchema();
      String tableComment = catalogBaseTable.getComment();

      // create a target iceberg table
      CatalogBaseTable table = new CatalogTableImpl(hiveTableSchema, null, tableComment);
      ObjectPath tableTarget = new ObjectPath(icebergDbName, icebergTableName);
      TableIdentifier identifier = TableIdentifier.of(icebergDbName, icebergTableName);

      icebergCatalog.createTable(identifier, FlinkSchemaUtil.convert(hiveTableSchema));
      Table icebergTable = icebergCatalog.loadTable(identifier);
      //把hive的数据导入到iceberg中
      importTable(icebergTable, hiveCatalog, tableSource);
    } catch (Exception e) {
      LOG.error("eee", e);
    }
    return null;
  }

  private void importTable(Table targetTable, HiveCatalog catalog, ObjectPath tableSource) throws Exception {

    org.apache.hadoop.hive.metastore.api.Table hiveTable = catalog.getHiveTable(tableSource);
    String location = hiveTable.getSd().getLocation();

    Configuration conf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
    Map<String, String> partition = Collections.emptyMap();
    PartitionSpec spec = PartitionSpec.unpartitioned();
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(targetTable.properties());
    String nameMappingString = targetTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping = nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

    //获取原始hive表的数据，构造成iceberg表的DataFile list
    List<DataFile> files = listOrcPartition(partition, location, spec, conf, metricsConfig, nameMapping);

    AppendFiles append = targetTable.newAppend();
    files.forEach(append::appendFile);
    append.commit();
  }


  private static List<DataFile> listOrcPartition(Map<String, String> partitionPath, String partitionUri,
                                                 PartitionSpec spec, Configuration conf,
                                                 MetricsConfig metricsSpec, NameMapping mapping) {
    try {
      Path partition = new Path(partitionUri);
      FileSystem fs = partition.getFileSystem(conf);

      return Arrays.stream(fs.listStatus(partition, HIDDEN_PATH_FILTER))
          .filter(FileStatus::isFile)
          .map(stat -> {
            Metrics metrics = OrcMetrics.fromInputFile(HadoopInputFile.fromPath(stat.getPath(), conf),
                metricsSpec, mapping);
            String partitionKey = spec.fields().stream()
                .map(PartitionField::name)
                .map(name -> String.format("%s=%s", name, partitionPath.get(name)))
                .collect(Collectors.joining("/"));

            return DataFiles.builder(spec)
                .withPath(stat.getPath().toString())
                .withFormat("orc")
                .withFileSizeInBytes(stat.getLen())
                .withMetrics(metrics)
                .withPartitionPath(partitionKey)
                .build();

          }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
