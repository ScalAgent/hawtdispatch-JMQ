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

import static java.lang.String.format;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.fusesource.hawtdispatch.CustomDispatchSource;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.EventAggregator;
import org.fusesource.hawtdispatch.OrderedEventAggregator;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.TaskWrapper;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class HawtCustomDispatchSource<Event, MergedEvent> extends AbstractDispatchObject implements CustomDispatchSource<Event, MergedEvent> {
    public static final boolean DEBUG = false;

    final AtomicBoolean canceled = new AtomicBoolean();
    private Task cancelHandler;
    private Task eventHandler;

    private final ThreadLocal<MergedEvent> outboundEvent = new ThreadLocal<MergedEvent>();
    private final ThreadLocal<MergedEvent> firedEvent = new ThreadLocal<MergedEvent>();
    private final EventAggregator<Event, MergedEvent> aggregator;
    private MergedEvent pendingEvent;
    private final boolean ordered;

    public HawtCustomDispatchSource(HawtDispatcher dispatcher, EventAggregator<Event, MergedEvent> aggregator, DispatchQueue queue) {
        this.aggregator = aggregator;
        this.suspended.incrementAndGet();
        this.ordered = aggregator instanceof OrderedEventAggregator;
        setTargetQueue(queue);
    }

    @Override
    public MergedEvent getData() {
        final MergedEvent rc = firedEvent.get();
        firedEvent.set(null);
        return rc;
    }

    protected final ConcurrentLinkedQueue<MergedEvent> externalQueue = new ConcurrentLinkedQueue<MergedEvent>();
    protected final AtomicLong size = new AtomicLong();

    @Override
    public void merge(Event event) {
        debug("merge called");
        WorkerThread thread = WorkerThread.currentWorkerThread();
        if( thread!=null ) {
            MergedEvent previous = outboundEvent.get();
            MergedEvent next = aggregator.mergeEvent(previous, event);
            if( next==null ) {
                debug("merge resulted in cancel");
                outboundEvent.remove();
            } else {
                outboundEvent.set(next);
                if( previous==null ) {
                    debug("first merge, posting deferred fire event");
                    if( ordered ) {
                        HawtDispatchQueue current = HawtDispatcher.CURRENT_QUEUE.get();
                        current.getSourceQueue().add(this);
                    } else {
                        thread.getDispatchQueue().getSourceQueue().add(this);
                    }
                } else {
                    debug("there was a previous merge, no need to post deferred fire event");
                }
            }
        } else {
            debug("merge not called from a worker thread.. triggering fire event now");
            fireEvent(aggregator.mergeEvent(null, event));
        }
    }

    @Override
    public void run() {
        debug("deferred fire event executing");
        fireEvent(outboundEvent.get());
        outboundEvent.remove();
    }

    private void fireEvent(final MergedEvent event) {
        if( event!=null ) {
            String taskName = Task.DEBUG_TASK ? "HawtCustomDispatchSource fire event for " + targetQueue.getLabel() : null;
            targetQueue.execute(new Task(taskName) {
                @Override
                public void run() {
                    if( isCanceled() ) {
                        debug("canceled");
                        return;
                    }
                    if( isSuspended() ) {
                        debug("fired.. but suspended");
                        synchronized(HawtCustomDispatchSource.this) {
                            if( pendingEvent==null ) {
                                pendingEvent = event;
                            } else {
                                pendingEvent = aggregator.mergeEvents(pendingEvent, event);
                            }
                        }
                    } else {
                        MergedEvent e=null;
                        synchronized(HawtCustomDispatchSource.this) {
                            e = pendingEvent;
                            pendingEvent = null;
                        }
                        if( e!=null ) {
                            debug("fired.. mergined with previous pending event..");
                            e = aggregator.mergeEvents(e, event);
                        } else {
                            debug("fired.. no previous pending event..");
                            e = event;
                        }
                        firedEvent.set(e);
                        try {
                            eventHandler.run();
                        } catch (Throwable e1) {
                            Thread thread = Thread.currentThread();
                            thread.getUncaughtExceptionHandler().uncaughtException(thread, e1);
                        }
                        firedEvent.remove();
                        debug("eventHandler done");
                    }
                }
            });
        }
    }

    @Override
    protected void onStartup() {
        if( eventHandler==null ) {
            throw new IllegalArgumentException("eventHandler must be set");
        }
        onResume();
    }

    @Override
    public void cancel() {
        if( canceled.compareAndSet(false, true) ) {
            String taskName = Task.DEBUG_TASK ? "HawtCustomDispatchSource cancel" : null;
            targetQueue.execute(new Task(taskName) {
                @Override
                public void run() {
                    if( cancelHandler!=null ) {
                        cancelHandler.run();
                    }
                }
            });
        }
    }

    @Override
    protected void onResume() {
        debug("onResume");
        String taskName = Task.DEBUG_TASK ? "HawtCustomDispatchSource resume" : null;
        targetQueue.execute(new Task(taskName) {
            @Override
            public void run() {
                if( isCanceled() ) {
                    return;
                }
                if( !isSuspended() ) {
                    MergedEvent e=null;
                    synchronized(HawtCustomDispatchSource.this) {
                        e = pendingEvent;
                        pendingEvent = null;
                    }
                    if( e!=null ) {
                        firedEvent.set(e);
                        eventHandler.run();
                        firedEvent.remove();
                    }
                }
            }
        });
    }

    @Override
    public boolean isCanceled() {
        return canceled.get();
    }

    @Override
    @Deprecated
    public void setCancelHandler(Runnable handler) {
        this.setCancelHandler(new TaskWrapper(handler));
    }

    @Override
    @Deprecated
    public void setEventHandler(Runnable handler) {
        this.setEventHandler(new TaskWrapper(handler));
    }

    @Override
    public void setCancelHandler(Task cancelHandler) {
        this.cancelHandler = cancelHandler;
    }

    @Override
    public void setEventHandler(Task eventHandler) {
        this.eventHandler = eventHandler;
    }

    protected void debug(String str, Object... args) {
        if (DEBUG) {
            String thread = Thread.currentThread().getName();
            System.out.println(format("DEBUG %1$tT.%1$tL | %2$s | HawtCustomDispatchSource %3$0#10x | %4$s", System.currentTimeMillis(), thread, System.identityHashCode(this), format(str, args)));
        }
    }

    protected void debug(Throwable thrown, String str, Object... args) {
        if (DEBUG) {
            if (str != null) {
                debug(str, args);
            }
            if (thrown != null) {
                thrown.printStackTrace();
            }
        }
    }

}
