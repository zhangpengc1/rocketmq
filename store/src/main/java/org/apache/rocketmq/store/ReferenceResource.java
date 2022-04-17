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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);

    protected volatile boolean available = true;

    protected volatile boolean cleanupOver = false;

    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 关闭mapperfile
     *
     * 初次调用时this.available为true， 设置available为false，并设置初次关闭的时间戳 (firstShutdownTimestamp)为当前时间戳。
     * 调用release()方法尝试释放资源，release只有在引用次数小于1的情况下才会释放资源。
     *
     * 如果引用次数大于0，对比当前时间与firstShutdownTimestamp，如果已经超过了其最大拒绝存活期，
     * 则每执行一次引用操作，引用数减少 1000，直到引用数小于0时通过执行realse()方法释放资源
     *
     * @param intervalForcibly
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());

                // 【重点】在整个MappedFile销毁的过程中，首先需要释放资源，释放资源的前提条件是该MappedFile的引用小于、等于0
                this.release();
            }
        }
    }

    /**
     * 释放资源
     *
     */
    public void release() {
        // 将引用次数减1，如果引用数小于等于0，则执行cleanup()方 法
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {
            // 重点
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * 判断是否清理完成
     *
     * 判断标准是引用次数小于、等于0并且cleanupOver为true，cleanupOver为true的触发条件是release成功将MappedByteBuffer资源释放了
     * @return
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
