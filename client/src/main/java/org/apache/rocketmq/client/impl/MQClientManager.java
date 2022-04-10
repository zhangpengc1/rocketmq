/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

public class MQClientManager {
    private final static InternalLogger log = ClientLogger.getLog();
    private static MQClientManager instance = new MQClientManager();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    // MQClientInstance缓存表
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable = new ConcurrentHashMap<String, MQClientInstance>();

    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }


    /**
     * 创建 MQClientManager实例，整个JVM实例中只存在一个MQClientManager实例
     *
     * 维护一个 MQClientInstance缓存表 ConcurrentMap<String/*clientId/，MQClientInstance> factoryTable =new ConcurrentHashMap<String， MQClientInstance>()，
     * 也就是同一个 clientId 只会创建一个MQClientInstance
     *
     * @param clientConfig
     * @param rpcHook
     * @return
     */
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        // clientId为客户端 IP+instance+(unitname可选)，如果在同一台物理服务器部署两个应用程序，岂不是clientId相同，会造成混乱?
        /**
         * 为了避免这个问题，
         * 如果 instance 为默认值 DEFAULT 的话，RocketMQ 会自动将 instance 设置为 进程ID，这样避免了不同进程的相互影响
         * 但同一个JVM 中的不同消费者和不同生产者在启动时获取到的MQClientInstance实例都是同一个。
         *
         * 根据后面的介绍 ，MQClientInstance封装了RocketMQ网络处理API，
         * 是消息生产者( Producer)、消息消费者 (Consumer)与 NameServer、 Broker打交道的网络通道。
         */
        String clientId = clientConfig.buildMQClientId();

        // 从缓存表中获取，没有的话就新建
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);

            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
