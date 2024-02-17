/**
 * Copyright (C) 2012 FuseSource, Inc.
 * http://fusesource.com
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

import org.fusesource.hawtdispatch.*;
import org.fusesource.hawtdispatch.internal.ThreadDQActiveMetricsCollector.ThreadDispatchQueueMetrics;
import org.fusesource.hawtdispatch.internal.pool.SimpleThread;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class ThreadDispatchQueue implements HawtDispatchQueue {

    volatile String label;

    final LinkedList<Task> localTasks = new LinkedList<Task>();
    final ConcurrentLinkedQueue<Task> sharedTasks = new ConcurrentLinkedQueue<Task>();
    final WorkerThread thread;
    final GlobalDispatchQueue globalQueue;
    private final LinkedList<Task> sourceQueue= new LinkedList<Task>();
    private volatile ThreadDQMetricsCollector metricsCollector = ThreadDQInactiveMetricsCollector.INSTANCE;
    private volatile boolean profile=false;

    public ThreadDispatchQueue(GlobalDispatchQueue globalQueue, WorkerThread thread) {
        this.thread = thread;
        this.globalQueue = globalQueue;
        this.label=thread.getName()+"_P-"+globalQueue.getLabel();
        checkCollector();
        getDispatcher().track(this);
    }

    public LinkedList<Task> getSourceQueue() {
        return sourceQueue;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public boolean isExecuting() {
        return globalQueue.dispatcher.getCurrentThreadQueue() == this;
    }

    public void assertExecuting() {
        assert isExecuting() : getDispatcher().assertMessage(getLabel());
    }

    public HawtDispatcher getDispatcher() {
        return globalQueue.dispatcher;
    }

    @Deprecated
    public void execute(Runnable runnable) {
        this.execute(new TaskWrapper(runnable));
    }

    @Deprecated
    public void executeAfter(long delay, TimeUnit unit, Runnable runnable) {
        this.executeAfter(delay, unit, new TaskWrapper(runnable));
    }

    public void execute(Task task) {
        // We don't have to take the synchronization hit
        Task mtask = metricsCollector.track(task);
        if( Thread.currentThread()!=thread ) {
            sharedTasks.add(mtask);
            thread.unpark();
        } else {
            localTasks.add(mtask);
        }
    }

    public Task poll() {
        Task rc = localTasks.poll();
        if (rc ==null) {
            rc = sharedTasks.poll();
        }
        return rc;
    }

    public void executeAfter(long delay, TimeUnit unit, Task task) {
        getDispatcher().timerThread.addRelative(task, this, delay, unit);
    }

    public void resume() {
        throw new UnsupportedOperationException();
    }

    public void suspend() {
        throw new UnsupportedOperationException();
    }

    public boolean isSuspended() {
        throw new UnsupportedOperationException();
    }

    public void setTargetQueue(DispatchQueue queue) {
        throw new UnsupportedOperationException();
    }
    
    public HawtDispatchQueue getTargetQueue() {
        return null;
    }
    
    public DispatchPriority getPriority() {
        return globalQueue.getPriority();
    }

    public GlobalDispatchQueue isGlobalDispatchQueue() {
        return null;
    }

    public SerialDispatchQueue isSerialDispatchQueue() {
        return null;
    }

    public ThreadDispatchQueue isThreadDispatchQueue() {
        return this;
    }

    public DispatchQueue createQueue(String label) {
        DispatchQueue rc = globalQueue.dispatcher.createQueue(label);
        rc.setTargetQueue(this);
        return rc;
    }

    public QueueType getQueueType() {
        return QueueType.THREAD_QUEUE;
    }

    public void profile(boolean profile) {
        this.profile = profile;
        checkCollector();
    }

    public boolean profile() {
        return this.profile;
    }

    private void checkCollector() {
        if( profile() || getDispatcher().profile() ) {
            if(  metricsCollector == ThreadDQInactiveMetricsCollector.INSTANCE ) {
                metricsCollector = new ThreadDQActiveMetricsCollector(this);
                getDispatcher().track(this);
            }
        } else {
            if(  metricsCollector != ThreadDQInactiveMetricsCollector.INSTANCE ) {
                metricsCollector = ThreadDQInactiveMetricsCollector.INSTANCE;
                getDispatcher().untrack(this);
            }
        }
    }

    public long setLastGlobalPoll(long now) {
      return metricsCollector.setLastGlobalPoll(now);
    }
    
    public long setLastSourcePoll(long now) {
      return metricsCollector.setLastSourcePoll(now);
    }
    
    public long setLastSelect(long now) {
      return metricsCollector.setLastSelect(now);
    }

    public long setParkTime(long now) {
      return metricsCollector.setParkTime(now);
    }
    
    public Metrics metrics() {
        Metrics baseMetrics = metricsCollector.metrics();
        if (baseMetrics == null)
          return null;
        ThreadDispatchQueueMetrics metrics = (ThreadDispatchQueueMetrics) baseMetrics;
        // fill in additional monitoring fields
        metrics.localTasksSize = localTasks.size();
        metrics.sharedTasksSize = sharedTasks.size();
        metrics.sourceQueueSize = sourceQueue.size();
        if (thread instanceof SimpleThread)
          metrics.poolTasksSize = ((SimpleThread) thread).getPoolTasksSize();
        return metrics;
    }

    public WorkerThread getThread() {
        return thread;
    }
}
