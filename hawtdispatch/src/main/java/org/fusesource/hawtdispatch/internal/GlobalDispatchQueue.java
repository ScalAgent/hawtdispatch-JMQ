/**
 * Copyright (C) 2012 FuseSource, Inc.
 * http://fusesource.com
 * Copyright (C) 2026 ScalAgent D.T
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

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.fusesource.hawtdispatch.DispatchPriority;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.Metrics;
import org.fusesource.hawtdispatch.ShutdownException;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.TaskWrapper;
import org.fusesource.hawtdispatch.internal.pool.SimplePool;
import org.fusesource.hawtdispatch.internal.util.IntrospectionSupport;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class GlobalDispatchQueue implements HawtDispatchQueue {

    public final HawtDispatcher dispatcher;
    volatile String label;
    private final DispatchPriority priority;
    final WorkerPool workers;

    public GlobalDispatchQueue(HawtDispatcher dispatcher, DispatchPriority priority, int threads) {
        this.dispatcher = dispatcher;
        this.priority = priority;
        this.label=priority.toString();
        this.workers = new SimplePool(this, threads, priority);
        dispatcher.track(this);
    }

    public void start() {
        workers.start();
    }

    public void shutdown() {
        workers.shutdown();
    }

    @Override
    public void shutdown(int level) {
      switch (level) {
      case 1:
        for (DispatchQueue threadQueue: getThreadQueues()) {
          threadQueue.shutdown(level);
        }
        break;
      case 2:
        // currently not called
        // the old shutdown() is called instead from HawtDispatcher
      }
    }

    @Override
    public HawtDispatcher getDispatcher() {
        return dispatcher;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public boolean isExecuting() {
        ThreadDispatchQueue tq = dispatcher.getCurrentThreadQueue();
        if( tq!=null ){
            return tq.globalQueue == this;
        }
        return false;
    }

    @Override
    public LinkedList<Task> getSourceQueue() {
        ThreadDispatchQueue tq = dispatcher.getCurrentThreadQueue();
        if( tq!=null ){
            return tq.getSourceQueue();
        }
        return null;
    }

    @Override
    public void assertExecuting() {
        assert isExecuting() : getDispatcher().assertMessage(getLabel());
    }

    @Override
    @Deprecated
    public void execute(final Runnable runnable) {
        execute(new TaskWrapper(runnable));
    }

    @Override
    @Deprecated()
    public void executeAfter(long delay, TimeUnit unit, Runnable runnable) {
        this.executeAfter(delay, unit, new TaskWrapper(runnable));
    }

    @Override
    public void execute(Task task) {
        if( dispatcher.shutdownState.get() > 1 ) {
            throw new ShutdownException();
        }
        workers.execute(task);
    }

    @Override
    public void executeAfter(long delay, TimeUnit unit, Task task) {
        if( dispatcher.shutdownState.get() > 0 ) {
            throw new ShutdownException();
        }
        dispatcher.timerThread.addRelative(task, this, delay, unit);
    }

    @Override
    public ThreadDispatchQueue getTargetQueue() {
        return null;
    }

    public DispatchPriority getPriority() {
        return priority;
    }

    @Override
    public void resume() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void suspend() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSuspended() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTargetQueue(DispatchQueue queue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GlobalDispatchQueue isGlobalDispatchQueue() {
        return this;
    }

    @Override
    public SerialDispatchQueue isSerialDispatchQueue() {
        return null;
    }

    @Override
    public ThreadDispatchQueue isThreadDispatchQueue() {
        return null;
    }

    @Override
    public String toString() {
        return IntrospectionSupport.toString(this);
    }

    @Override
    public DispatchQueue createQueue(String label) {
        DispatchQueue rc = dispatcher.createQueue(label);
        rc.setTargetQueue(this);
        return rc;
    }

    @Override
    public QueueType getQueueType() {
        return QueueType.GLOBAL_QUEUE;
    }

    DispatchQueue[] getThreadQueues() {
        WorkerThread[] threads = workers.getThreads();
        DispatchQueue []rc = new DispatchQueue[threads.length];
        for(int i=0;i < threads.length; i++){
            rc[i] = threads[i].getDispatchQueue();
        }
        return rc;
    }

    @Override
    public void profile(boolean profile) {
    }

    @Override
    public boolean profile() {
        return false;
    }


    @Override
    public Metrics metrics() {
        return null;
    }

}
