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
package org.apache.rocketmq.client.consumer.store;

import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Offset store interface
 * 消费进度设计思考
 *
 * 广播模式下，消息消费进度的存储与消费组无关，集群模式下则以主题与消费组为键，保存该主题所有队列的消费进度。
 * 结合并发消息消费的整个流程，思考一下并发消息消费关于消息进度更新的问题
 *
 * 1)消费者线程池每处理完一个消息消费任务 (ConsumeRequest)，会从ProcessQueue中移除本批消费的消息，
 * 并返回ProcessQueue中最小的偏移量，用该偏移量更新消息队列消费进 度，也就是说更新消费进度与消费任务中的消息没有关系。
 * 例如现在 有两个消费任务task1(queueOffset分别为20、40)和 task2(queueOffset分别为50、70)，
 * 并且ProcessQueue中当前包含最小消息偏移量为10的消息，则task2消费结束后，将使用10更新消费 进度，而不是70。
 * 当task1消费结束后，还是以10更新消息队列消费进度，消息消费 进度的推进取决于ProcessQueue中偏移量最小的消息消费速度。
 * 如果偏移量为10的消息消费成功，且ProcessQueue中包含消息偏移量为100的消息，
 * 则消息偏移量为10的消息消费成功后，将直接用100更新消息消费进度。如果在消费消息偏移量为10的消息时发生了死锁，会导致消息一直无法被消费
 * 岂不是消息进度无法向前推进了?是的，为了 避免这种情况，RocketMQ引入了一种消息拉取流控措施: DefaultMQPushConsumer#consumeConcurrentlyMaxSpan=2000，消息处 理队列ProcessQueue中最大消息偏移与最小偏移量不能超过该值，如 果超过该值，将触发流控，延迟该消息队列的消息拉取。
 *
 *
 */
public interface OffsetStore {
    /**
     * Load
     *
     * 从消息进度存储文件加载消息进度到内存。
     */
    void load() throws MQClientException;

    /**
     * 更新内存中的消息消费进度。
     *
     *
     * @param mq 消息消费队列。
     * @param offset 消息消费偏移量。
     * @param increaseOnly true表示offset必须大于内存中当前的消费偏移 量才更新。
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * Get offset from local storage
     *
     * 读取消息消费进度
     *
     *
     * @param mq 消息消费队列
     * @param type 读取方式，可选值包括 READ_FROM_MEMORY，即从内存中读取，READ_FROM_STORE，即从磁盘中读取，MEMORY_FIRST_THEN_STORE，即先从内存中读取，再从 磁盘中读取。
     * @return
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * Persist all offsets,may be in local storage or remote name server
     * 持久化指定消息 队列进度到磁盘。
     *
     * Set messageQueue:消息队列集合。
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * Persist the offset,may be in local storage or remote name server
     */
    void persist(final MessageQueue mq);

    /**
     * Remove offset
     * 将消息队列的消息消费进度从内存中移除。
     *
     *
     */
    void removeOffset(MessageQueue mq);

    /**
     * 复制该主题下所有消息队列的消息消费进度。
     *
     * @return The cloned offset table of given topic
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * 使用集群模式更新存储在Broker端的消息消费进度。
     *
     * @param mq
     * @param offset
     * @param isOneway
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;
}
