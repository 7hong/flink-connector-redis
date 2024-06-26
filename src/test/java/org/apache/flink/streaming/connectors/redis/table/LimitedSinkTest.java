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

package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_COMMAND;

/**
 * @Author: Jeff Zou @Date: 2022/9/27 15:08
 */
public class LimitedSinkTest extends TestRedisConfigBase {

    @Test
    public void testLimitedSink() throws Exception {
        singleRedisCommands.del("sink_limit_test");
        final int ttl = 60000;
        String sink =
                "create table sink_redis(key_name varchar, user_name VARCHAR, passport varchar) with ( 'connector'='redis', "
                        + "'host'='"
                        + REDIS_HOST
                        + "','port'='"
                        + REDIS_PORT
                        + "', 'redis-mode'='single','password'='"
                        + REDIS_PASSWORD
                        + "','"
                        + REDIS_COMMAND
                        + "'='"
                        + RedisCommand.HSET
                        + "', 'sink.limit'='true', 'sink.limit.max-online'='"
                        + ttl
                        + "','sink.limit.max-num'='10')";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(sink);

        String source =
                "create table source_table (user_name VARCHAR, passport varchar) with ('connector'= 'datagen','rows-per-second'='1',"
                        + " 'fields.user_name.kind'='sequence', 'fields.user_name.start'='0', 'fields.user_name.end'='100',"
                        + " 'fields.passport.kind'='sequence',  'fields.passport.start'='0', 'fields.passport.end'='100')";
        tEnv.executeSql(source);

        try {
            tEnv.executeSql(
                    "insert into sink_redis select 'sink_limit_test', user_name, passport from source_table ")
                    .getJobClient()
                    .get()
                    .getJobExecutionResult()
                    .get();
        } catch (Exception e) {
        }

        Preconditions.condition(singleRedisCommands.hget("sink_limit_test", "0").equals("0"), "");
        Preconditions.condition(singleRedisCommands.hget("sink_limit_test", "51") == null, "");

        Thread.sleep(ttl + 10000);
        Preconditions.condition(singleRedisCommands.hget("sink_limit_test", "0") == null, "");
    }
}
