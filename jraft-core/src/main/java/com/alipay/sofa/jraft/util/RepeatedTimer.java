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
package com.alipay.sofa.jraft.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.util.timer.HashedWheelTimer;
import com.alipay.sofa.jraft.util.timer.Timeout;
import com.alipay.sofa.jraft.util.timer.Timer;
import com.alipay.sofa.jraft.util.timer.TimerTask;

/**
 * Repeatable timer based on java.util.Timer.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-30 3:45:37 PM
 */
public abstract class RepeatedTimer implements Describer {

    public static final Logger LOG  = LoggerFactory.getLogger(RepeatedTimer.class);

    private final Lock         lock = new ReentrantLock();
    //timer是HashedWheelTimer
    private final Timer        timer;
    //实例是HashedWheelTimeout
    private Timeout            timeout;
    private boolean            stopped;
    private volatile boolean   running;
    private boolean            destroyed;
    private boolean            invoking;
    private volatile int       timeoutMs;
    private final String       name;

    public int getTimeoutMs() {
        return this.timeoutMs;
    }

    //name代表RepeatedTimer实例的种类，timeoutMs是超时时间
    public RepeatedTimer(String name, int timeoutMs) {
        //其实JRaft的定时任务调度器是基于Netty的时间轮来做的，如果没有看过Netty的源码，很可能并不知道时间轮算法，也就很难想到要去使用这么优秀的定时调度算法了。
        this(name, timeoutMs, new HashedWheelTimer(new NamedThreadFactory(name, true), 1, TimeUnit.MILLISECONDS, 2048));
    }

    public RepeatedTimer(String name, int timeoutMs, Timer timer) {
        super();
        this.name = name;
        this.timeoutMs = timeoutMs;
        this.stopped = true;
        this.timer = Requires.requireNonNull(timer, "timer");
    }

    /**
     * Subclasses should implement this method for timer trigger.
     */
    protected abstract void onTrigger();

    /**
     * Adjust timeoutMs before every scheduling.
     *
     * @param timeoutMs timeout millis
     * @return timeout millis
     */
    protected int adjustTimeout(final int timeoutMs) {
        return timeoutMs;
    }

    /**
     * 这个run方法会由timer进行回调，如果没有调用stop或destroy方法的话，那么调用完onTrigger方法后会继续调用schedule，然后一次次循环调用RepeatedTimer的run方法。
     * 如果调用了destroy方法，在这里会有一个onDestroy的方法，可以由实现类override复写执行一个钩子。
     */
    public void run() {
        //加锁，只能一个线程调用这个方法
        this.lock.lock();
        try {
            //表示RepeatedTimer已经被调用过
            this.invoking = true;
        } finally {
            this.lock.unlock();
        }
        try {
            //然后会调用RepeatedTimer实例实现的方法
            onTrigger();
        } catch (final Throwable t) {
            LOG.error("Run timer failed.", t);
        }
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            this.invoking = false;
            //如果调用了stop方法，那么将不会继续调用schedule方法
            if (this.stopped) {
                this.running = false;
                invokeDestroyed = this.destroyed;
            } else {
                this.timeout = null;
                schedule();
            }
        } finally {
            this.lock.unlock();
        }
        if (invokeDestroyed) {
            onDestroy();
        }
    }

    /**
     * Run the timer at once, it will cancel the timer and re-schedule it.
     */
    public void runOnceNow() {
        this.lock.lock();
        try {
            if (this.timeout != null && this.timeout.cancel()) {
                this.timeout = null;
                run();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Called after destroy timer.
     */
    protected void onDestroy() {
        // NO-OP
    }

    /**
     * Start the timer.
     */
    public void start() {
        //加锁，只能一个线程调用这个方法
        this.lock.lock();
        try {
            //destroyed默认是false
            if (this.destroyed) {
                return;
            }
            //stopped在构造器中初始化为ture
            if (!this.stopped) {
                return;
            }
            //启动完一次后下次就无法再次往下继续
            this.stopped = false;
            if (this.running) {
                return;
            }
            //running默认为false
            this.running = true;
            //从上面的赋值以及加锁的情况来看，这个是只能被调用一次的。然后会调用到schedule方法中
            schedule();
        } finally {
            this.lock.unlock();
        }
    }

    private void schedule() {
        if(this.timeout != null) {
            this.timeout.cancel();
        }
        /*如果timer调用了TimerTask的run方法，那么便会回调到RepeatedTimer的run方法中：*/
        final TimerTask timerTask = timeout -> {
            try {
                RepeatedTimer.this.run();
            } catch (final Throwable t) {
                LOG.error("Run timer task failed, taskName={}.", RepeatedTimer.this.name, t);
            }
        };
        this.timeout = this.timer.newTimeout(timerTask, adjustTimeout(this.timeoutMs), TimeUnit.MILLISECONDS);
    }

    /**
     * Reset timer with new timeoutMs.
     *
     * @param timeoutMs timeout millis
     */
    public void reset(final int timeoutMs) {
        this.lock.lock();
        this.timeoutMs = timeoutMs;
        try {
            if (this.stopped) {
                return;
            }
            if (this.running) {
                schedule();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Reset timer with current timeoutMs
     */
    public void reset() {
        this.lock.lock();
        try {
            reset(this.timeoutMs);
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Destroy timer
     */
    public void destroy() {
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            this.destroyed = true;
            if (!this.running) {
                invokeDestroyed = true;
            }
            // Timer#stop is idempotent
            this.timer.stop();
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                if (this.timeout.cancel()) {
                    invokeDestroyed = true;
                    this.running = false;
                }
                this.timeout = null;
            }
        } finally {
            this.lock.unlock();
            if (invokeDestroyed) {
                onDestroy();
            }
        }
    }

    /**
     * Stop timer
     */
    public void stop() {
        this.lock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                this.timeout.cancel();
                this.running = false;
                this.timeout = null;
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        final String _describeString;
        this.lock.lock();
        try {
            _describeString = toString();
        } finally {
            this.lock.unlock();
        }
        out.print("  ") //
            .println(_describeString);
    }

    @Override
    public String toString() {
        return "RepeatedTimer [timeout=" + this.timeout + ", stopped=" + this.stopped + ", running=" + this.running
               + ", destroyed=" + this.destroyed + ", invoking=" + this.invoking + ", timeoutMs=" + this.timeoutMs
               + "]";
    }
}
