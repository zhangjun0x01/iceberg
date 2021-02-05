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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.Action;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateAction implements Action<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(MigrateAction.class);

  private static final String PATQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
  private static final String ORC_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
  private static final String AVRO_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
  private static final PathFilter HIDDEN_PATH_FILTER =
      p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");

  private final StreamExecutionEnvironment env;
  private final HiveCatalog flinkHiveCatalog; // the HiveCatalog of flink
  private final String hiveSourceDbName;
  private final String hiveSourceTableName;
  private final Catalog icebergCatalog;
  private final Namespace baseNamespace;
  private final String icebergDbName;
  private final String icebergTableName;
  private int maxParallelism;

  public MigrateAction(StreamExecutionEnvironment env, HiveCatalog flinkHiveCatalog, String hiveSourceDbName,
                       String hiveSourceTableName, Catalog icebergCatalog, Namespace baseNamespace,
                       String icebergDbName,
                       String icebergTableName) {
    this.env = env;
    this.flinkHiveCatalog = flinkHiveCatalog;
    this.hiveSourceDbName = hiveSourceDbName;
    this.hiveSourceTableName = hiveSourceTableName;
    this.icebergCatalog = icebergCatalog;
    this.baseNamespace = baseNamespace;
    this.icebergDbName = icebergDbName;
    this.icebergTableName = icebergTableName;
    this.maxParallelism = env.getParallelism();
  }

  @Override
  public Integer execute() {
    // hive source table
    ObjectPath tableSource = new ObjectPath(hiveSourceDbName, hiveSourceTableName);
    org.apache.hadoop.hive.metastore.api.Table hiveTable;
    flinkHiveCatalog.open();
    try {
      hiveTable = flinkHiveCatalog.getHiveTable(tableSource);
    } catch (TableNotExistException e) {
      throw new RuntimeException(String.format("The source table %s not exists ! ", hiveSourceTableName));
    }

    List<FieldSchema> fieldSchemaList = hiveTable.getSd().getCols();
    List<FieldSchema> partitionList = hiveTable.getPartitionKeys();
    fieldSchemaList.addAll(partitionList);
    Schema icebergSchema = HiveSchemaUtil.convert(fieldSchemaList);
    PartitionSpec spec = toPartitionSpec(partitionList, icebergSchema);

    String hiveFormat = hiveTable.getSd().getInputFormat();
    FileFormat fileFormat;
    switch (hiveFormat) {
      case PATQUET_INPUT_FORMAT:
        fileFormat = FileFormat.PARQUET;
        break;

      case ORC_INPUT_FORMAT:
        fileFormat = FileFormat.ORC;
        break;

      case AVRO_INPUT_FORMAT:
        fileFormat = FileFormat.AVRO;
        break;

      default:
        throw new UnsupportedOperationException("Unsupported file format");
    }

    TableIdentifier identifier = TableIdentifier.of(toNamespace(baseNamespace), icebergTableName);
    String hiveLocation = hiveTable.getSd().getLocation();

    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    if (!baseNamespace.isEmpty()) {
      properties.put(FlinkCatalogFactory.BASE_NAMESPACE, baseNamespace.toString());
    }

    Table icebergTable;
    if (icebergCatalog instanceof HadoopCatalog) {
      icebergTable = icebergCatalog.createTable(identifier, icebergSchema, spec, properties);
    } else {
      icebergTable = icebergCatalog.createTable(identifier, icebergSchema, spec, hiveLocation, properties);
    }

    String nameMapping =
        PropertyUtil.propertyAsString(icebergTable.properties(), TableProperties.DEFAULT_NAME_MAPPING, null);
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(icebergTable.properties());

    List<DataFile> files = null;
    if (spec.isUnpartitioned()) {
      MigrateMap migrateMap = new MigrateMap(spec, nameMapping, fileFormat, metricsConfig);
      DataStream<PartitionAndLocation> dataStream =
          env.fromElements(new PartitionAndLocation(hiveLocation, Maps.newHashMap()));
      DataStream<List<DataFile>> ds = dataStream.map(migrateMap);
      try {
        files = Lists.newArrayList(ds.executeAndCollect("migrate table :" + icebergTable.name())).stream()
            .flatMap(Collection::stream).collect(Collectors.toList());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      try {
        files = importPartitions(spec, tableSource, nameMapping, fileFormat, metricsConfig, icebergTable.name());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    AppendFiles append = icebergTable.newAppend();
    files.forEach(append::appendFile);
    append.commit();
    return files.size();
  }

  private Namespace toNamespace(Namespace namespace) {
    if (namespace.isEmpty()) {
      return Namespace.of(icebergDbName);
    } else {
      String[] namespaces = new String[baseNamespace.levels().length + 1];
      System.arraycopy(baseNamespace.levels(), 0, namespaces, 0, baseNamespace.levels().length);
      namespaces[baseNamespace.levels().length] = icebergDbName;
      return Namespace.of(namespaces);
    }
  }

  private List<DataFile> importPartitions(PartitionSpec spec, ObjectPath tableSource,
                                          String nameMapping, FileFormat fileFormat,
                                          MetricsConfig metricsConfig, String hiveTableName)
      throws Exception {
    List<CatalogPartitionSpec> partitionSpecs = flinkHiveCatalog.listPartitions(tableSource);
    List<PartitionAndLocation> inputs = Lists.newArrayList();
    for (CatalogPartitionSpec partitionSpec : partitionSpecs) {
      Partition partition =
          flinkHiveCatalog.getHivePartition(flinkHiveCatalog.getHiveTable(tableSource), partitionSpec);
      inputs.add(
          new PartitionAndLocation(partition.getSd().getLocation(), Maps.newHashMap(partitionSpec.getPartitionSpec())));
    }

    int size = partitionSpecs.size();
    int parallelism = Math.min(size, maxParallelism);

    DataStream<PartitionAndLocation> dataStream = env.fromCollection(inputs);
    MigrateMap migrateMap = new MigrateMap(spec, nameMapping, fileFormat, metricsConfig);
    DataStream<List<DataFile>> ds = dataStream.map(migrateMap).setParallelism(parallelism);
    return Lists.newArrayList(ds.executeAndCollect("migrate table :" + hiveTableName)).stream()
        .flatMap(Collection::stream).collect(Collectors.toList());
  }

  private static class MigrateMap implements MapFunction<PartitionAndLocation, List<DataFile>> {
    private final PartitionSpec spec;
    private final String nameMappingString;
    private final FileFormat fileFormat;
    private final MetricsConfig metricsConfig;

    MigrateMap(PartitionSpec spec, String nameMapping, FileFormat fileFormat, MetricsConfig metricsConfig) {
      this.spec = spec;
      this.nameMappingString = nameMapping;
      this.fileFormat = fileFormat;
      this.metricsConfig = metricsConfig;
    }

    @Override
    public List<DataFile> map(PartitionAndLocation map) {
      Map<String, String> partitions = map.getMap();
      String location = map.getLocation();
      Configuration conf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
      NameMapping nameMapping = nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
      List<DataFile> files;
      switch (fileFormat) {
        case PARQUET:
          files = listParquetPartition(partitions, location, spec, conf, metricsConfig, nameMapping);
          break;

        case ORC:
          files = listOrcPartition(partitions, location, spec, conf, metricsConfig, nameMapping);
          break;

        default:
          throw new UnsupportedOperationException("Unsupported file format");
      }

      return files;
    }

    private List<DataFile> listOrcPartition(Map<String, String> partitionPath, String partitionUri,
                                            PartitionSpec partitionSpec, Configuration conf,
                                            MetricsConfig metricsSpec, NameMapping mapping) {
      try {
        Path partition = new Path(partitionUri);
        FileSystem fs = partition.getFileSystem(conf);

        return Arrays.stream(fs.listStatus(partition, HIDDEN_PATH_FILTER))
            .filter(FileStatus::isFile)
            .map(stat -> {
              Metrics metrics = OrcMetrics.fromInputFile(HadoopInputFile.fromPath(stat.getPath(), conf),
                  metricsSpec, mapping);
              String partitionKey = partitionSpec.fields().stream()
                  .map(PartitionField::name)
                  .map(name -> String.format("%s=%s", name, partitionPath.get(name)))
                  .collect(Collectors.joining("/"));

              return DataFiles.builder(partitionSpec)
                  .withPath(stat.getPath().toString())
                  .withFormat(FileFormat.ORC.name())
                  .withFileSizeInBytes(stat.getLen())
                  .withMetrics(metrics)
                  .withPartitionPath(partitionKey)
                  .build();

            }).collect(Collectors.toList());
      } catch (IOException e) {
        throw new UncheckedIOException(String.format("Unable to list files in partition: %s", partitionUri), e);
      }
    }

    private static List<DataFile> listParquetPartition(Map<String, String> partitionPath, String partitionUri,
                                                       PartitionSpec spec, Configuration conf,
                                                       MetricsConfig metricsSpec, NameMapping mapping) {
      try {
        Path partition = new Path(partitionUri);
        FileSystem fs = partition.getFileSystem(conf);

        return Arrays.stream(fs.listStatus(partition, HIDDEN_PATH_FILTER))
            .filter(FileStatus::isFile)
            .map(stat -> {
              Metrics metrics;
              try {
                ParquetMetadata metadata = ParquetFileReader.readFooter(conf, stat);
                metrics = ParquetUtil.footerMetrics(metadata, Stream.empty(), metricsSpec, mapping);
              } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Unable to read the footer of the parquet file: %s", stat.getPath()), e);
              }

              String partitionKey = spec.fields().stream()
                  .map(PartitionField::name)
                  .map(name -> String.format("%s=%s", name, partitionPath.get(name)))
                  .collect(Collectors.joining("/"));

              return DataFiles.builder(spec)
                  .withPath(stat.getPath().toString())
                  .withFormat(FileFormat.PARQUET.name())
                  .withFileSizeInBytes(stat.getLen())
                  .withMetrics(metrics)
                  .withPartitionPath(partitionKey)
                  .build();

            }).collect(Collectors.toList());
      } catch (IOException e) {
        throw new UncheckedIOException(String.format("Unable to list files in partition: %s", partitionUri), e);
      }
    }
  }

  public static class PartitionAndLocation implements java.io.Serializable {
    private String location;
    private Map<String, String> map;

    public PartitionAndLocation(String location, Map<String, String> map) {
      this.location = location;
      this.map = map;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public Map<String, String> getMap() {
      return map;
    }

    public void setMap(Map<String, String> map) {
      this.map = map;
    }
  }

  private PartitionSpec toPartitionSpec(List<FieldSchema> partitionKeys, Schema icebergSchema) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
    for (FieldSchema partitionKey : partitionKeys) {
      builder = builder.identity(partitionKey.getName());
    }
    return builder.build();
  }

  public MigrateAction maxParallelism(int parallelism) {
    Preconditions.checkArgument(parallelism > 0, "Invalid max parallelism %d", parallelism);
    this.maxParallelism = parallelism;
    return this;
  }
}
