/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.cdc;

import akka.remote.serialization.ProtobufSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.flink.actions.Actions;

public class TestRewriteDataFileCDC {
  public static void main(String[] args) throws TableNotExistException {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 使用 Kryo 注册 Google Protobuf 序列化器
    env.getConfig().registerTypeWithKryoSerializer(BaseCombinedScanTask.class, new ProtobufSerializer());
    env.setParallelism(1);
    env.enableCheckpointing(10000);
    StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
    tenv.executeSql(
        "CREATE CATALOG iceberg WITH (\n"
            + "  'type'='iceberg',\n"
            + "  'catalog-type'='hive',"
            + "   'warehouse'='hdfs://10.160.85.185/user/hive2/warehouse',\n"
            + "   'uri'='thrift://10.160.85.186:9083'"
            + ")");

    FlinkCatalog flinkCatalog = (FlinkCatalog) tenv.getCatalog("iceberg").get();
    Table icebergTable = flinkCatalog.loadIcebergTable(new ObjectPath("iceberg_db", "iceberg_cdc_test10"));

    Actions.forTable(env, icebergTable).rewriteDataFiles().execute();
  }
}
