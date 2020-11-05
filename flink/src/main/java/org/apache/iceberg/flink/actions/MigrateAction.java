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

import java.util.List;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Action;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateAction implements Action {
  private static final Logger LOG = LoggerFactory.getLogger(MigrateAction.class);

  private HiveCatalog hiveCatalog;
  private Catalog icebergCatalog;
  private String[] baseNamespace;
  private FlinkCatalog flinkCatalog;


  @Override
  public Object execute() {

    ObjectPath tablePath = null;

    try {
      flinkCatalog.createTable(tablePath, null, false);
      TableIdentifier identifier = null;
      Table table = icebergCatalog.loadTable(identifier);

      importTable(table, hiveCatalog);
    } catch (Exception e) {
      LOG.error("eee", e);
    }


    return null;
  }

  private void importTable(Table table, HiveCatalog catalog) throws Exception {

    // load hive table

    ObjectPath tablePath = new ObjectPath("", "");
    org.apache.hadoop.hive.metastore.api.Table hiveTable = catalog.getHiveTable(tablePath);
    String location = hiveTable.getSd().getLocation();


  }

  TableIdentifier toIdentifier(ObjectPath path) {
    return TableIdentifier.of(toNamespace(path.getDatabaseName()), path.getObjectName());
  }

  private Namespace toNamespace(String database) {
    String[] namespace = new String[baseNamespace.length + 1];
    System.arraycopy(baseNamespace, 0, namespace, 0, baseNamespace.length);
    namespace[baseNamespace.length] = database;
    return Namespace.of(namespace);
  }

  private static PartitionSpec toPartitionSpec(List<String> partitionKeys, Schema icebergSchema) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
    partitionKeys.forEach(builder::identity);
    return builder.build();
  }
}
