package com.cbxg.sql.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author:cbxg
 * @date:2021/4/10
 * @description: hive catalog 示例
 */
public class HiveCatalog01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String catalogName = "hiveCatalog01";
        String database = "test1";
        String hiveConfDir = "D:/gitProjects/flink_sql_tutorials/src/main/resources/conf";
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, database, hiveConfDir);
        tableEnv.registerCatalog(catalogName,hiveCatalog);
        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase(database);
//        tableEnv.sqlQuery("select * from person limit 10").execute().print();
        tableEnv.executeSql("insert into  person partition(dt='2021-07-20') values('ls',20)");
    }
}
