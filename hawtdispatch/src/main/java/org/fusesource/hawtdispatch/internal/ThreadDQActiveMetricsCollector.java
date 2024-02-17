/**
 * Copyright (C) 2024 ScalAgent D.T
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fusesource.hawtdispatch.internal;

import org.fusesource.hawtdispatch.Metrics;
import org.fusesource.hawtdispatch.Task;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 *
 */
final public class ThreadDQActiveMetricsCollector extends ThreadDQMetricsCollector {

    private final ThreadDispatchQueue queue;
    private final ActiveMetricsCollector baseCollector;

    private volatile long lastGlobalPoll;
    private volatile long lastSourcePoll;
    private volatile long lastSelect;
    private final AtomicLong max_park_time = new AtomicLong();
    private final AtomicLong total_park_time = new AtomicLong();
    
    public ThreadDQActiveMetricsCollector(ThreadDispatchQueue queue) {
        this.queue = queue;
        baseCollector = new ActiveMetricsCollector(queue);
    }

    public Task track(final Task runnable) {
      return baseCollector.track(runnable);
    }

    public class ThreadDispatchQueueMetrics extends Metrics {
        /**
         * Size of the localTasks queue, filled in by the ThreadDispatchQueue.
         */
        int localTasksSize;
        /**
         * Size of the sharedTasks queue, filled in by the ThreadDispatchQueue.
         */
        int sharedTasksSize;
        /**
         * Size of the shared tasks queue of the worker pool, filled in by the ThreadDispatchQueue.
         */
        int poolTasksSize;
        /**
         * Size of the sourceQueue queue, filled in by the ThreadDispatchQueue.
         */
        int sourceQueueSize;
        
        /**
         * Delay since the SimpleThread polled the global pool tasks queue.
         */
        long lastGlobalPollNS;
        /**
         * Delay since the SimpleThread polled the source queue.
         */
        long lastSourcePollNS;
        /**
         * Delay since the SimpleThread got parked in select operation.
         */
        long lastSelectNS;

        /**
         * The longest amount of time at thread parked in select operation.
         */
        public long maxParkedTimeNS;

        /**
         * The sum of all the time spent in select operation.
         */
        public long totalParkedTimeNS;

        /**
         * Builds a specific Metrics from a standard Metrics.
         */
        ThreadDispatchQueueMetrics(Metrics base) {
            date = base.date;
            durationNS = base.durationNS;
            queue = base.queue;
            enqueued = base.enqueued;
            dequeued = base.dequeued;
            maxWaitTimeNS = base.maxWaitTimeNS;
            maxRunTimeNS = base.maxRunTimeNS;
            totalRunTimeNS = base.totalRunTimeNS;
            totalWaitTimeNS = base.totalWaitTimeNS;
            lastGlobalPollNS = date - ThreadDQActiveMetricsCollector.this.lastGlobalPoll;
            lastSourcePollNS = date - ThreadDQActiveMetricsCollector.this.lastSourcePoll;
            lastSelectNS = date - ThreadDQActiveMetricsCollector.this.lastSelect;
            maxParkedTimeNS = max_park_time.getAndSet(0);
            totalParkedTimeNS = total_park_time.getAndSet(0);
        }
        @Override
        public String toString() {
            return String.format("{%s, localTasks_size:%d, sharedTask_size:%d, poolTasks_size:%d, sourceQueue_size:%d, last_global_poll_time:%.2f ms, last_source_poll_time:%.2f ms, last_select_time:%.2f ms, max_park_time:%.2f ms, total_park_time:%.2f ms }",
                    super.toString(),
                    localTasksSize,
                    sharedTasksSize,
                    poolTasksSize,
                    sourceQueueSize,
                    lastGlobalPollNS / 1000000.0f,
                    lastSourcePollNS / 1000000.0f,
                    lastSelectNS / 1000000.0f,
                    maxParkedTimeNS / 1000000.0f,
                    totalParkedTimeNS / 1000000.0f);
        }
    }
    public Metrics metrics() {
        Metrics baseMetrics = baseCollector.metrics();
        if (baseMetrics == null)
          return null;
        return new ThreadDispatchQueueMetrics(baseMetrics);
    }

    public long setLastGlobalPoll(long now) {
      // if input date is null, capture current date
      if (now == 0) {
        now = System.nanoTime();
      }
      lastGlobalPoll = now;
      return now;
    }
    
    public long setLastSourcePoll(long now) {
      // if input date is null, capture current date
      if (now == 0) {
        now = System.nanoTime();
      }
      lastSourcePoll = now;
      return now;
    }
    
    public long setLastSelect(long now) {
      // if input date is null, capture current date
      if (now == 0) {
        now = System.nanoTime();
      }
      lastSelect = now;
      return now;
    }

    public long setParkTime(long now) {
      // if input date is null, capture current date
      if (now == 0) {
        now = System.nanoTime();
      }
      long parkTime = now - lastSelect;
      total_park_time.addAndGet(parkTime);
      setMax(max_park_time, parkTime);
      
      return now;
    }

    private void setMax(AtomicLong holder, long value) {
        while (true) {
            long p = holder.get();
            if( value > p ) {
                if( holder.compareAndSet(p, value) ) {
                    return;
                }
            } else {
                return;
            }
        }
    }

}
