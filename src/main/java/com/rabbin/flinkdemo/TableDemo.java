/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rabbin.flinkdemo;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class TableDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'filesystem',\n" +
                "    'path'= '/Users/rabbin/CS/tools/flink/projects/FlinkDemo/src/main/resources/transactions.json',\n" +
                "    'format' = 'json'\n"+
                ")");
        tEnv.executeSql("CREATE TABLE transactions_result (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'filesystem',\n" +
                "    'path'= '/Users/rabbin/CS/tools/flink/projects/FlinkDemo/src/main/resources/transactions_result.json',\n" +
                "    'format' = 'json'\n"+
                ")");

        Table table = tEnv.sqlQuery("select * from transactions");
        System.out.println(table.explain());
        TableResult result = table.executeInsert("transactions_result");
        System.out.println("await");
        result.await();
    }
}
