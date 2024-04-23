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

package org.apache.flink.streaming.connectors.redis.config;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SENTINEL;

/** */
public class FlinkSentinelConfigHandler implements FlinkConfigHandler {

    @Override
    public FlinkConfigBase createFlinkConfig(ReadableConfig config) {
        String masterName = config.get(RedisOptions.REDIS_MASTER_NAME);
        String sentinelsInfo = config.get(RedisOptions.SENTINELS_INFO);
        String sentinelsPassword =
                StringUtils.isNullOrWhitespaceOnly(config.get(RedisOptions.SENTINELS_PASSWORD))
                        ? null
                        : config.get(RedisOptions.SENTINELS_PASSWORD);
        Preconditions.checkNotNull(masterName, "master should not be null in sentinel mode");
        Preconditions.checkNotNull(sentinelsInfo, "sentinels should not be null in sentinel mode");

        LettuceConfig lettuceConfig =
                new LettuceConfig(
                        config.get(RedisOptions.NETTY_IO_POOL_SIZE),
                        config.get(RedisOptions.NETTY_EVENT_POOL_SIZE));

        FlinkSentinelConfig flinkSentinelConfig =
                new FlinkSentinelConfig.Builder()
                        .setSentinelsInfo(sentinelsInfo)
                        .setMasterName(masterName)
                        .setConnectionTimeout(config.get(RedisOptions.TIMEOUT))
                        .setDatabase(config.get(RedisOptions.DATABASE))
                        .setPassword(config.get(RedisOptions.PASSWORD))
                        .setSentinelsPassword(sentinelsPassword)
                        .setLettuceConfig(lettuceConfig)
                        .build();
        return flinkSentinelConfig;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_SENTINEL);
        return require;
    }

    public FlinkSentinelConfigHandler() {
    }
}
