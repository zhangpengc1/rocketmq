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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    // 是否是顺序消息
    private boolean orderTopic = false;

    private boolean haveTopicRouterInfo = false;

    // 该主题队列的消息队列
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();

    // 每选择一次消息队列，该值会自增 l，如果 Integer.MAX_V ALUE,则重置为 0，用于选择消息队列。
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    // topic 队列元数据
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    // 默认发送机制

    /**
     *  首先在一次消息发送过程中，可能会多次执行选择消息队列这个方法，lastBrokerName 就是上一次选择的执行发送消息失败的Broker。
     *
     *  第一次执行消息队列选择时， lastBrokerName 为 null，此时直接用 sendWhichQueue 自增再获取值 ， 与当前路由表中消息队列个数取模，
     *  返回该位置的 MessageQueue(selectOneMessageQueue()方法)，如果消息发送再失败的话，
     *  下次进行 消息队列选择 时规避上次 MesageQueue 所 在的 Broker，否则还是很有可能再次失败。
     *
     * 该算法在一次消息发送过程中能成功规避故障的Broker，但如果Broker若机，由于路由算法中的消息队列是按Broker排序的，
     * 如果上一次根据路由算法选择的是若机的 Broker 的第一个 队列 ，那么随后的 下 次选择的是若 机 Broker 的第二个队列，消息发送很有可能会
     * 失败，再次 引发 重试，带来不必要的性能损耗。
     *
     * 那么有什么方法在一次消息发送失败后，暂时将该 Broker 排除在消息队列选择范围外呢?或许有朋友会问，Broker不可用后，路由信息中为什么还会包含该Broker的路由信息呢?
     * 其实这不难解释:首先， NameServer 检 测 Broker是否可用是有延迟的，最短为一次心跳检测间 隔(1Os); 其次， NameServer不会 检测到 Broker岩机后马上推送消息给消息生产者，而是消息生产者每隔 30s更新一次路由 信息，所以消息生产者最快感知 Broker最新的路由信息也需要 30s。 如果能引人一种机制，
     * 在 Broker若机期间，如果一次消息发送失败后，可以将该 Broker暂时排除在消息队列的选 择范围中 。
     *
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        /**
         *  第一次执行消息队列选择时， lastBrokerName 为 null，此时直接用 sendWhichQueue 自增再获取值，与当前路由表中消息队列个数取模，
         *  返回该位置的 MessageQueue(selectOneMessageQueue()方法)，如果消息发送再失败的话，
         *  下次进行 消息队列选择 时规避上次 MesageQueue 所 在的 Broker，否则还是很有可能再次失败。
         */
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {

            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int pos = Math.abs(index++) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
