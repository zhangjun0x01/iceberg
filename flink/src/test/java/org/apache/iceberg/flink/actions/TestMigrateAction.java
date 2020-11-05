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
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.junit.Test;

public class TestMigrateAction {

  @Test
  public void test1() throws DatabaseNotExistException {
    HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", "/Users/user/work/hive/conf");
    System.out.println(hiveCatalog);

    List<String> list = hiveCatalog.listTables("default");
    list.stream().forEach(System.out::println);

  }

  @Test
  public void testMigrate() {
    HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", "/Users/user/work/hive/conf");
    hiveCatalog.open();
    String hiveSourceDbName = "default";
    String hiveSourceTableName = "orc_test7";

    Catalog icebergCatalog =
        new org.apache.iceberg.hive.HiveCatalog("hive", "hdfs://10.160.85.185/user/hive/warehouse", 2,
            new Configuration());
    String icebergDbName = "iceberg_db";
    String icebergTableName = "iceberg_orc_test7";
    MigrateAction action = new MigrateAction(hiveCatalog, hiveSourceDbName, hiveSourceTableName,
        icebergCatalog, icebergDbName, icebergTableName);
    action.execute();
  }
}
