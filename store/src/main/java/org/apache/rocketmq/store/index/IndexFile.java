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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;

    // hashSlot 数量
    private final int hashSlotNum;

    private final int indexNum;

    private final MappedFile mappedFile;

    private final FileChannel fileChannel;

    private final MappedByteBuffer mappedByteBuffer;

    // index文件头
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 存入index文件
     *
     * @param key 消息索引
     * @param phyOffset 消息物理偏移量
     * @param storeTimestamp 消息存储时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 当前已使用条目大于、等于允许最大条目数时，返回fasle，表示当前Index文件已写满。

        // 如果当前index文件未写满，则根据key算出哈希码。
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 根据keyHash对哈希槽数量取余定位到哈希码对应的哈希槽下标
            int keyHash = indexKeyHashMethod(key); // abs(key.hashcode)
            // keyHash 和 hashSlotNum取模
            int slotPos = keyHash % this.hashSlotNum;
            // 得出槽的位置。哈希码对应的哈希槽的物理地址为IndexHeader(40字节)加上下标乘以每个哈希槽的大小(4字节)
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {
                fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,false);

                // 读取哈希槽中存储的数据，如果哈希槽存储的数据小于0 或大于当前Index文件中的索引条目，则将slotValue设置为0
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 计算待存储消息的时间戳与第一条消息时间戳的差值，并转换成秒
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 将条目信息存储在Index文件中
                /**
                 * 1)计算新添加条目的起始物理偏移量:
                 *      头部字节长度 + 哈希槽数量×单个哈希槽大小(4个字节)+当前Index条目个数×单个Index条 目大小(20个字节)。
                 *
                 * 2)依次将哈希码、消息物理偏移量、消息存储时间戳与Index文 件时间戳、当前哈希槽的值存入MappedByteBuffer。
                 *
                 * 3)将当前Index文件中包含的条目数量存入哈希槽中，覆盖原先哈希槽的值。
                 *
                 * 以上是哈希冲突链式解决方案的关键实现，哈希槽中存储的是该哈希码对应的最新Index条目的下标，
                 * 新的Index条目最后4个字节存储该哈希码上一个条目的Index下标。
                 * 如果哈希槽中存储的值为0或大于当前Index文件最大条目数或小于-1，表示该哈希槽当前并没有与之对应的Index条目。
                 * 值得注意的是，Index文件条目中存储的不是消息索引key，而是消息属性key的哈希，在根据key查找时需要根据消息物理偏移量找到消息，
                 * 进而验证消息key的值。之所以只存储哈希，而不存储具体的key，是为了将Index条目设计为定长结构，才能方便地检索与定位条目
                 */
                // 1.计算新添加条目的起始物理偏移量:
                // 头部字节长度 + 哈希槽数量×单个哈希槽大小(4个字节)+当前Index条目个数×单个Index条 目大小(20个字节)。
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                // 2)依次将哈希码、消息物理偏移量、消息存储时间戳与Index文件时间戳、当前哈希槽的值存入MappedByteBuffer。
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                // 3.将当前Index文件中包含的条目数量存入哈希槽中，覆盖原先哈希槽的值。
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 更新文件索引头信息。
                // 如果当前文件只包含一个条目， 则更新beginPhyOffset、beginTimestamp、endPyhOffset、 endTimestamp以及当前文件使用索引条目等信息
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 根据索引key查找消息
     *
     *
     * @param phyOffsets 查找到的消息物理偏移量
     * @param key 索引key
     * @param maxNum 本次查找最大消息条数
     * @param begin 开始时间戳
     * @param end 结束时间戳
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            // 根据key算出key的哈希码，keyHash对哈希槽数量取余，
            // 定位到哈希码对应的哈希槽下标，哈希槽的物理地址为 IndexHeader(40字节)加上下标乘以每个哈希槽的大小(4字节)
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                // 如果对应的哈希槽中存储的数据小于1或大于当前索引条目个数，表示该哈希码没有对应的条目，直接返回
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {

                    // 因为会存在哈希冲突，所以根据slotValue定位该哈希槽最新的一个Item条目，
                    // 将存储的物理偏移量加入phyOffsets，然后继续验证Item条目中存储的上一个Index下标，
                    // 如果大于、等于1并且小 于当前文件的最大条目数，则继续查找，否则结束查找
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        // 根据Index下标定位到条目的起始物理偏移量，然后依次读取哈希码、物理偏移量、时间戳、上一个条目的Index下标
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        // 如果存储的时间戳小于0，则直接结束查找。
                        // 如果哈希匹 配并且消息存储时间介于待查找时间start、end之间，
                        // 则将消息物理偏移量加入phyOffsets，并验证条目的前一个Index索引，
                        // 如果索引大 于、等于1并且小于Index条目数，则继续查找，否则结束查找
                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
