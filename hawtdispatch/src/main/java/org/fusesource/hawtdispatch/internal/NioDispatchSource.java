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
import static org.fusesource.hawtdispatch.DispatchQueue.QueueType.THREAD_QUEUE;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.DispatchSource;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.TaskWrapper;

/**
 * <p>
 * Implements the DispatchSource interface.
 * </p>
 * <p>
 * Description: An NioDispatchSource is associated with one SelectableChannel
 * but supports being registered on selectors associated with different thread.
 * Usually just one at time tho.
 * </p>
 *
 * @author cmacnaug
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class NioDispatchSource extends AbstractDispatchObject implements DispatchSource {

    public static final boolean DEBUG = false;

    final SelectableChannel channel;
    volatile DispatchQueue selectorQueue;

    final AtomicBoolean canceled = new AtomicBoolean();
    final int interestOps;

    Task cancelHandler;
    Task eventHandler;

    // These fields are only accessed by the ioManager's thread.
    public static class KeyState {
        int readyOps;
        final NioAttachment attachment;

        public SelectionKey key() {
            return attachment.key();
        }

        public KeyState(NioAttachment attachment) {
            this.attachment = attachment;
        }

        @Override
        public String toString() {
            return "{ready: "+opsToString(readyOps)+" }";
        }
    }

    private static String opsToString(int ops) {
        ArrayList sb = new ArrayList();
        if( (ops & SelectionKey.OP_ACCEPT) != 0) {
            sb.add("ACCEPT");
        }
        if( (ops & SelectionKey.OP_CONNECT) != 0) {
            sb.add("CONNECT");
        }
        if( (ops & SelectionKey.OP_READ) != 0) {
            sb.add("READ");
        }
        if( (ops & SelectionKey.OP_WRITE) != 0) {
            sb.add("WRITE");
        }
        return sb.toString();
    }

    final ThreadLocal<KeyState> keyState = new ThreadLocal<KeyState>();

    public NioDispatchSource(HawtDispatcher dispatcher, SelectableChannel channel, int interestOps, DispatchQueue targetQueue) {
        if( interestOps == 0 ) {
            throw new IllegalArgumentException("invalid interest ops");
        }
        this.channel = channel;
        this.selectorQueue = pickThreadQueue(dispatcher, targetQueue);
        this.interestOps = interestOps;
        this.suspended.incrementAndGet();
        this.setTargetQueue(targetQueue);
    }


    static private DispatchQueue pickThreadQueue(HawtDispatcher dispatcher, DispatchQueue targetQueue) {
        // Try to select a thread queue associated /w the target if available..
        DispatchQueue selectorQueue = targetQueue;
        while( selectorQueue.getQueueType()!=THREAD_QUEUE  && selectorQueue.getTargetQueue() !=null ) {
            selectorQueue = selectorQueue.getTargetQueue();
        }

        // otherwise.. pick the thread queue with the fewest registered selection
        // keys.
        if( selectorQueue.getQueueType()!=THREAD_QUEUE ) {

            WorkerThread[] threads = dispatcher.DEFAULT_QUEUE.workers.getThreads();
            WorkerThread min = threads[0];
            int minSize = min.getNioManager().getRegisteredKeyCount();
            for( int i=1; i < threads.length; i++) {
                int s = threads[i].getNioManager().getRegisteredKeyCount();
                if( s < minSize ) {
                    minSize = s;
                    min = threads[i];
                }
            }
            selectorQueue = min.getDispatchQueue();
        }

        return selectorQueue;
    }

    @Override
    protected void onStartup() {
        if( eventHandler==null ) {
            throw new IllegalArgumentException("eventHandler must be set");
        }
        register_on(selectorQueue);
    }

    @Override
    public void cancel() {
        if( canceled.compareAndSet(false, true) ) {
            String taskName = Task.DEBUG_TASK ? "NioDispatchSource internal_cancel for " + targetQueue.getLabel() : null;
            selectorQueue.execute(new Task(taskName) {
                @Override
                public void run() {
                    internal_cancel();
                }
            });
        }
    }

    /**
     * Undoes every nio operations related to this source, then upcall the cancelHandler.
     *
     * This function may be called from above or from below.
     * From above, it is called by cancel, during the canceling of the transport.
     * From below, it is called by NioManager.cancel as an exception handler from nio select related operations.
     */
    void internal_cancel() {
        // the key_cancel part actually performs the nio undoing. This is done in 1 or 2 steps, depending on the cases.
        // In both cases, key_cancel is executed as an atomic operation, as it runs as a single Task in the selectorQueue.
        // It is also idempotent, as it runs only when keyState is not null, and nullifies keyState.
        //
        // In the first case, the transport has created its readSource and writeSource on the same ThreadDispatchQueue,
        // and this internal_cancel is executed on one of those queues while the other is still active.
        // In this case, internal_cancel only breaks the uplink from the nio event to the source eventHandler.
        // More specifically, the source is removed from the list of sources in the selection key attachment.
        // The nio selection key itself is untouched and could trigger an nio event. However the NioManager
        // will no longer be able to execute the source event handler.
        //
        // In the second case, the two sources have been created on separate ThreadDispatchQueues, or the other
        // source has already been canceled. In this case, internal_cancel will actually perform all the nio undoing
        // in addition to the cleaning of the uplink described in the first case. If both sources are running in the same
        // queue, then the nio SelectionKey is shared and the nio undoing will work for both sources.
        key_cancel();
        // this second part relates to the asynchronous cancel algorithm of the transport.
        // The upcall is always executed, each time internal_cancel is called. However the call to internal_cancel
        // is protected by the value of the AtomicBoolean canceled, so that it will ever be called once for each source.
        if( cancelHandler!=null ) {
            targetQueue.execute(cancelHandler);
        }
    }

    private NioManager getCurrentNioManager() {
        return WorkerThread.currentWorkerThread().getNioManager();
    }

    private void key_cancel() {
        // Deregister...
        KeyState state = keyState.get();
        if( state==null ) {
            return;
        }
        debug("canceling source");
        state.attachment.sources.remove(this);
        if( state.attachment.sources.isEmpty() ) {
            debug("canceling key.");
            getCurrentNioManager().cancel(state.key());
        }
        keyState.remove();
    }

    private void register_on(final DispatchQueue queue) {
        String taskName = Task.DEBUG_TASK ? "NioDispatchSource register interest for " + targetQueue.getLabel() : null;
        queue.execute(new Task(taskName){
            @Override
            public void run() {
                assert keyState.get()==null;
                if(DEBUG) debug("Registering interest %s", opsToString(interestOps));
                try {
                    NioAttachment attachment = getCurrentNioManager().register(channel, interestOps);
                    attachment.sources.add(NioDispatchSource.this);
                    keyState.set(new KeyState(attachment));

                } catch (ClosedChannelException | RuntimeException e) {
                    debug(e, "could not register with selector");
                    // the only possible source of the exception is from the register call
                    // the NioManager already canceled the key, and it may have started the
                    // asynchronous cancel algorithm if the key is shared with another source
                    // however this source has not been registered in the attachment, so it
                    // must be explicitely canceled.
                    if(canceled.compareAndSet(false, true)) {
                        internal_cancel();
                    }
                }
                debug("Registered");
            }
        });
    }


    public void fire(final int readyOps) {
        final KeyState state = keyState.get();
        if( state==null ) {
            return;
        }
        state.readyOps |= readyOps;
        if( state.readyOps!=0  && !isSuspended()&& !isCanceled() ) {
            state.readyOps = 0;
            String taskName = Task.DEBUG_TASK ? "NioDispatchSource fire " + targetQueue.getLabel() : null;
            targetQueue.execute(new Task(taskName) {
                @Override
                public void run() {
                    if( !isSuspended() && !isCanceled()) {
                        if(DEBUG) debug("fired %s", opsToString(readyOps));
                        try {
                            eventHandler.run();
                        } catch (Throwable e) {
                          Thread thread = Thread.currentThread();
                          thread.getUncaughtExceptionHandler().uncaughtException(thread, e);
                        }
                        updateInterest();
                    }
                }
            });
        }
    }

    /**
     * Task updating the interestOps of the source's SelectionKey.
     * This task must be run in the selectorQueue, which is ensured by the function updateInterest.
     */
    private final Task updateInterestTask = new Task("NioDispatchSource update interest") {
        @Override
        public void run() {
          if(isSuspended() || isCanceled())
            return;
          if(DEBUG) debug("adding interest: %s", opsToString(interestOps));
          KeyState state = keyState.get();
          if( state==null ) {
            // should never occur as isSuspended is false, so resume has already been called,
            // and the first call to resume calls onStartup which creates the KeyState
            if(DEBUG) debug("unexpected null keyState");
            return;
          }

          SelectionKey key = state.key();
          try {
            key.interestOps(key.interestOps() | interestOps);
          } catch(RuntimeException e) {
            // the expected exception is CancelledKeyException, however we want to make sure that
            // all exceptions are caught
            // the former call to internal_cancel looks wrong
            // as the key is canceled, NioManager.cancel must be called instead
            getCurrentNioManager().cancel(key);
          }
        }
    };

    /**
     * Executes the updateInterestTask in the proper queue.
     */
    private void updateInterest() {
        if( isCurrent(selectorQueue) ) {
            updateInterestTask.run();
        } else {
            selectorQueue.execute(updateInterestTask);
        }
    }

    private boolean isCurrent(DispatchQueue q) {
        WorkerThread thread = WorkerThread.currentWorkerThread();
        if( thread == null )
            return false;
        return thread.getDispatchQueue() == q;
    }

    @Override
    protected void onSuspend() {
        debug("onSuspend");
        super.onSuspend();
    }

    @Override
    protected void onResume() {
        debug("onResume");
        if( isCurrent(selectorQueue) ) {
            KeyState state = keyState.get();
            // state should not be null as the first call to resume calls onStartup which creates the KeyState
            if( state==null || state.readyOps==0 ) {
                updateInterest();
            } else {
                fire(state.readyOps);
            }
        } else {
            String taskName = Task.DEBUG_TASK ? "NioDispatchSource onResume for " + targetQueue.getLabel() : null;
            selectorQueue.execute(new Task(taskName){
                @Override
                public void run() {
                    KeyState state = keyState.get();
                    if( state==null || state.readyOps==0 ) {
                        updateInterest();
                    } else {
                        fire(interestOps);
                    }
                }
            });
        }
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

    public Void getData() {
        return null;
    }

    @Override
    public void setTargetQueue(DispatchQueue next) {
        super.setTargetQueue(next);

        // The target thread queue might be different. Optimize by switching the selector to it.
        // Do we need to switch selector threads?
        DispatchQueue queue = next;
        while( queue.getQueueType()!=THREAD_QUEUE  && queue.getTargetQueue() !=null ) {
            queue = queue.getTargetQueue();
        }
        if( queue.getQueueType()==THREAD_QUEUE && queue!=selectorQueue ) {
            DispatchQueue previous = selectorQueue;
            final DispatchQueue newQueue = queue;
            debug("Switching to " + newQueue.getLabel());
            selectorQueue = queue;
            if( previous!=null ) {
                previous.execute(new Task(){
                    @Override
                    public void run() {
                        key_cancel();
                        register_on(newQueue);
                    }
                });
            } else {
                register_on(newQueue);
            }
        }
    }

    protected void debug(String str, Object... args) {
        if (DEBUG) {
            String thread = Thread.currentThread().getName();
            String target ="";
            if( Dispatch.getCurrentQueue()!=null ) {
                target = Dispatch.getCurrentQueue().getLabel() + " | ";
            }
            System.out.println(format("DEBUG %1$tT.%1$tL | %2$s | NioDispatchSource #%3$0#10x | %4$s%5$s", System.currentTimeMillis(), thread, System.identityHashCode(this), target, format(str, args)));
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
