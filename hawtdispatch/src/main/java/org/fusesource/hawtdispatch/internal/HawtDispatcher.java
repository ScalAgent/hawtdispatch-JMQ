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

import static org.fusesource.hawtdispatch.DispatchPriority.DEFAULT;

import java.nio.channels.SelectableChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.fusesource.hawtdispatch.CustomDispatchSource;
import org.fusesource.hawtdispatch.DispatchPriority;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.DispatchSource;
import org.fusesource.hawtdispatch.Dispatcher;
import org.fusesource.hawtdispatch.EventAggregator;
import org.fusesource.hawtdispatch.Metrics;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.jmx.JmxService;


/**
 * Implements a simple dispatch system.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class HawtDispatcher implements Dispatcher {

  public static final Logger logger = Logger.getLogger("org.fusesource.hawtdispatch.HawtDispatcher");
  public static final boolean DEBUG  = logger.isLoggable(Level.FINE);

    public final static ThreadLocal<HawtDispatchQueue> CURRENT_QUEUE = new ThreadLocal<HawtDispatchQueue>();

    final GlobalDispatchQueue DEFAULT_QUEUE;
    private final Object HIGH_MUTEX = new Object();
    private GlobalDispatchQueue HIGH_QUEUE;
    private final Object LOW_MUTEX = new Object();
    private GlobalDispatchQueue LOW_QUEUE;

    private final String label;
    volatile TimerThread timerThread;

    private final int threads;
    private volatile boolean profile;
    final int drains;
    final boolean jmx;
    final AtomicInteger shutdownState = new AtomicInteger(0);

    volatile Thread.UncaughtExceptionHandler uncaughtExceptionHandler=null;

    public HawtDispatcher(DispatcherConfig config) {
        this.threads = config.getThreads();
        this.label = config.getLabel();
        this.profile = config.isProfile();
        this.drains = config.getDrains();
        this.jmx = config.isJmx();

        if( this.jmx ) {
            try {
                JmxService.register(this);
            } catch (Throwable ignore) {
            }
        }

        DEFAULT_QUEUE = new GlobalDispatchQueue(this, DispatchPriority.DEFAULT, config.getThreads());
        DEFAULT_QUEUE.start();
        DEFAULT_QUEUE.profile(profile);

        timerThread = new TimerThread(this);
        timerThread.start();
    }

    @Override
    public void shutdown() {

        // shutdown == 1 stop new dispatch after requests..
        if( shutdownState.compareAndSet(0, 1) ) {
            if (DEBUG)
              logger.fine("shutdown 1");
            // shutdown all NioManagers first, as that will trigger more tasks
            DEFAULT_QUEUE.shutdown(1);
            if( LOW_QUEUE!=null )
              LOW_QUEUE.shutdown(1);
            if(HIGH_QUEUE!=null)
              HIGH_QUEUE.shutdown(1);

            // give every one a chance to notice
            // the state change.
            sleep(100);
            timerThread.shutdown(new Task() {
                @Override
                public void run() {
                    // all outstanding timers will have been
                    // queued for execution


                    // shutdown == 2 stop queues from accepting
                    // new executions
                    shutdownState.set(2);
                    if (DEBUG)
                      logger.fine("shutdown 2");
                    // new state change..
                    sleep(100);

                    // Wait for the execution queues to drain..
                    if (DEBUG)
                      logger.fine("shutdown global dispatch queue");
                    DEFAULT_QUEUE.shutdown();
                    if( LOW_QUEUE!=null ) {
                        LOW_QUEUE.shutdown();
                    }
                    if(HIGH_QUEUE!=null) {
                        HIGH_QUEUE.shutdown();
                    }

                    // shutdown == 3 means we are fully drained.
                    shutdownState.set(3);
                    if (DEBUG)
                      logger.fine("shutdown 3");
                }
            }, DEFAULT_QUEUE);

            try {
              int timeout = Integer.getInteger("org.fusesource.hawtdispatch.shutdownTimeout", 5000);
              timerThread.join(timeout);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
        }

        if( this.jmx ) {
            try {
                JmxService.unregister(this);
            } catch (Throwable ignore) {
            }
        }

    }

    private void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void restart() {
        if( shutdownState.compareAndSet(3, 0) ) {
            timerThread = new TimerThread(this);
            timerThread.start();
            DEFAULT_QUEUE.start();
            if( LOW_QUEUE!=null ) {
                LOW_QUEUE.start();
            }
            if(HIGH_QUEUE!=null) {
                HIGH_QUEUE.start();
            }
        } else {
            throw new IllegalStateException("Not shutdown yet.");
        }
    }

    @Override
    public DispatchQueue getGlobalQueue() {
        return getGlobalQueue(DEFAULT);
    }

    @Override
    public GlobalDispatchQueue getGlobalQueue(DispatchPriority priority) {
        switch (priority) {
            case DEFAULT:
                return DEFAULT_QUEUE;
            case HIGH:
                // lazy load the high queue to avoid creating it's thread if the application is not using
                // the queue at all.
                synchronized(HIGH_MUTEX) {
                    if( HIGH_QUEUE==null ) {
                        HIGH_QUEUE = new GlobalDispatchQueue(this, DispatchPriority.HIGH, threads);
                        HIGH_QUEUE.start();
                        HIGH_QUEUE.profile(profile);
                    }
                    return HIGH_QUEUE;
                }
            case LOW:
                // lazy load the low queue to avoid creating it's thread if the application is not using
                // the queue at all.
                synchronized(LOW_MUTEX) {
                    if( LOW_QUEUE==null ) {
                        LOW_QUEUE = new GlobalDispatchQueue(this, DispatchPriority.LOW, threads);
                        LOW_QUEUE.start();
                        LOW_QUEUE.profile(profile);
                    }
                    return LOW_QUEUE;
                }
        }
        throw new AssertionError("switch missing case");
    }

    @Override
    public SerialDispatchQueue createQueue(String label) {
        SerialDispatchQueue rc = new SerialDispatchQueue(label);
        if (DEBUG)
          logger.fine("create queue: " + rc);
        rc.setTargetQueue(getGlobalQueue());
        rc.profile(profile);
        return rc;
    }

    @Override
    public DispatchSource createSource(SelectableChannel channel, int interestOps, DispatchQueue queue) {
        NioDispatchSource rc = new NioDispatchSource(this, channel, interestOps, queue);
        if (DEBUG)
          logger.fine("create source: " + rc);
        return rc;
    }

    @Override
    public <Event, MergedEvent> CustomDispatchSource<Event, MergedEvent> createSource(EventAggregator<Event, MergedEvent> aggregator, DispatchQueue queue) {
        HawtCustomDispatchSource rc = new HawtCustomDispatchSource(this, aggregator, queue);
        if (DEBUG)
          logger.fine("create source: " + rc);
        return rc;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public DispatchQueue getCurrentQueue() {
        return CURRENT_QUEUE.get();
    }


    @Override
    public ThreadDispatchQueue getCurrentThreadQueue() {
        WorkerThread thread = WorkerThread.currentWorkerThread();
        if( thread ==null ) {
            return null;
        }
        return thread.getDispatchQueue();
    }

    @Override
    public DispatchQueue[] getThreadQueues(DispatchPriority priority) {
        return getGlobalQueue(priority).getThreadQueues();
    }


    final static public WeakHashMap<HawtDispatchQueue, Object> queues = new WeakHashMap<HawtDispatchQueue, Object>();

    void track(HawtDispatchQueue queue) {
        synchronized (queues) {
            queues.put(queue, Boolean.TRUE);
        }
    }

    void untrack(HawtDispatchQueue queue) {
        synchronized (queues) {
            queues.remove(queue);
        }
    }

    @Override
    public void profile(boolean on) {
        profile = on;
    }

    @Override
    public boolean profile() {
        return profile;
    }

    @Override
    public List<Metrics> metrics() {
        synchronized (queues) {
            ArrayList<Metrics> rc = new ArrayList<Metrics>();
            for( HawtDispatchQueue queue : queues.keySet() ) {
                if( queue!=null ) {
                    Metrics metrics = queue.metrics();
                    if( metrics!=null ) {
                        rc.add(metrics);
                    }
                }
            }
            return rc;
        }

    }

    String assertMessage(String label) {
        StringBuilder sb = new StringBuilder();
        sb.append("Dispatch queue '");
        if(label!=null ) {
            sb.append(label);
        } else {
            sb.append("<no-label>");
        }
        sb.append("' was not executing, (currently executing: '");
        DispatchQueue q = getCurrentQueue();
        if( q!=null ) {
            if( q.getLabel()!=null ) {
                sb.append(q.getLabel());
            } else {
                sb.append("<no-label>");
            }
        } else {
            sb.append("<not-dispatched>");
        }
        sb.append("')");
        return sb.toString();
    }


    public Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return uncaughtExceptionHandler;
    }

    public void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    }
}
