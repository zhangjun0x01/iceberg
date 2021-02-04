package org.apache.iceberg.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;

public class TestCDC2Iceberg {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

//    tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg.iceberg_db.iceberg_cdc_05(\n" +
//        "    id int COMMENT 'unique id',\n" +
//        "    data STRING) \n" +
//        "WITH (\n" +
//        "'connector'='iceberg',\n" +
//        "'equality.field.columns'='id'\n" +
//        ")");


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

//    tenv.executeSql("select * from mysql_binlog").print();
    tenv.executeSql("insert into iceberg.iceberg_db.iceberg_cdc_05 select * from mysql_binlog");
  }
}
