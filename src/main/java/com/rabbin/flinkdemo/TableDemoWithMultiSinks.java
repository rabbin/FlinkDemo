package com.rabbin.flinkdemo;

import org.apache.flink.table.api.*;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.api.Expressions.$;

public class TableDemoWithMultiSinks {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        final Schema schema = Schema.newBuilder()
                .column("count", DataTypes.INT())
                .column("word", DataTypes.STRING())
                .build();

        tEnv.createTemporaryTable("MySource1", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/Users/rabbin/CS/tools/flink/projects/FlinkDemo/src/main/resources/source/path1.json")
                .format("json")
                .build());
        tEnv.createTemporaryTable("MySource2", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/Users/rabbin/CS/tools/flink/projects/FlinkDemo/src/main/resources/source/path2.json")
                .format("json")
                .build());
        tEnv.createTemporaryTable("MySink1", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/Users/rabbin/CS/tools/flink/projects/FlinkDemo/src/main/resources/sink/path1")
                .format("csv")
                .build());
        tEnv.createTemporaryTable("MySink2", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/Users/rabbin/CS/tools/flink/projects/FlinkDemo/src/main/resources/sink/path2")
                .format("csv")
                .build());

        StatementSet stmtSet = tEnv.createStatementSet();

        Table table1 = tEnv.from("MySource1").where($("word").like("F%"));
        stmtSet.addInsert("MySink1", table1);

        Table table2 = table1.unionAll(tEnv.from("MySource2"));
        stmtSet.addInsert("MySink2", table2);

//        System.out.println(stmtSet.explain());
        TableResult result = stmtSet.execute();
        result.await();
    }
}
