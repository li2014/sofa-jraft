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
package com.alipay.sofa.jraft.closure;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Closure queue implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-28 11:44:01 AM
 */
public class ClosureQueueImpl implements ClosureQueue {

    private static final Logger LOG = LoggerFactory.getLogger(ClosureQueueImpl.class);

    private final Lock          lock;

    /**
     * 队列头
     */
    private long                firstIndex;
    /**
     * 双向链表队列先进后出
     */
    private LinkedList<Closure> queue;

    @OnlyForTest
    public long getFirstIndex() {
        return firstIndex;
    }

    @OnlyForTest
    public LinkedList<Closure> getQueue() {
        return queue;
    }

    public ClosureQueueImpl() {
        super();
        this.lock = new ReentrantLock();
        this.firstIndex = 0;
        this.queue = new LinkedList<>();
    }

    /**
     * 清空不是丢弃，而是取出所有都执行
     */
    @Override
    public void clear() {
        List<Closure> savedQueue;
        this.lock.lock();
        try {
            this.firstIndex = 0;
            savedQueue = this.queue;
            this.queue = new LinkedList<>();
        } finally {
            this.lock.unlock();
        }

        final Status status = new Status(RaftError.EPERM, "Leader stepped down");
        for (final Closure done : savedQueue) {
            if (done != null) {
                //线程中执行 Permission issue leader总统下台了
                Utils.runClosureInThread(done, status);
            }
        }
    }

    @Override
    public void resetFirstIndex(final long firstIndex) {
        this.lock.lock();
        try {
            Requires.requireTrue(this.queue.isEmpty(), "Queue is not empty.");
            this.firstIndex = firstIndex;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void appendPendingClosure(final Closure closure) {
        this.lock.lock();
        try {
            //在链表尾部添加一个元素
            this.queue.add(closure);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long popClosureUntil(final long endIndex, final List<Closure> closures) {
        return popClosureUntil(endIndex, closures, null);
    }

    @Override
    public long popClosureUntil(final long endIndex, final List<Closure> closures, final List<TaskClosure> taskClosures) {
        closures.clear();
        if (taskClosures != null) {
            taskClosures.clear();
        }
        this.lock.lock();
        try {
            final int queueSize = this.queue.size();
            if (queueSize == 0 || endIndex < this.firstIndex) {
                return endIndex + 1;
            }
            if (endIndex > this.firstIndex + queueSize - 1) {
                LOG.error("Invalid endIndex={}, firstIndex={}, closureQueueSize={}", endIndex, this.firstIndex,
                    queueSize);
                return -1;
            }
            final long outFirstIndex = this.firstIndex;
            for (long i = outFirstIndex; i <= endIndex; i++) {
                //移除队列头第一元素
                final Closure closure = this.queue.pollFirst();
                if (taskClosures != null && closure instanceof TaskClosure) {
                    taskClosures.add((TaskClosure) closure);
                }
                closures.add(closure);
            }
            this.firstIndex = endIndex + 1;
            return outFirstIndex;
        } finally {
            this.lock.unlock();
        }
    }
}
