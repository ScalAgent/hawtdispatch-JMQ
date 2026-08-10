/**
 * Copyright (C) 2012 FuseSource, Inc.
 * http://fusesource.com
 * Copyright (C) 2022 - 2026 ScalAgent D.T
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

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 *
 * Notes de lecture par SL
 *
 *
 * In nio, there is a Selector, which allows I/O event detection to be multiplexed.
 * The Selector has a list of channels it will listen to, which it references using SelectionKeys.
 * In addition to the channel, the SelectionKey has a list of interestOps specifying the events to listen for on the channel.
 * To enable integration with user code, nio allows a user object to be associated with a SelectionKey;
 * for hawtdispatch, these are NioAttachments.
 * The NioAttachment allows you to trace back to the NioDispatchSource, which is the class for readSource and writeSource.
 * As these two sources share the same channel, and there is only one channel per NioAttachment,
 * the NioAttachment maintains a list of NioDispatchSource instances. The events expected by the two sources
 * are distinct and stored within the NioDispatchSource. The interestOps stored at the nio level
 * (in the SelectionKey) is a combination of both.
 * This class, NioManager, manages the nio/hawtdispatch mapping. In short:
 *
 * - nio
 * -- Selector
 * --- SelectionKey
 * ---- channel
 * ---- interestOps
 * ---- NioAttachement
 * - hawtdispatch
 * -- NioDispatchSource
 * --- channel
 * --- interestOps
 * --- NioAttachment (via a ThreadLocal<KeyState>)
 * -- NioManager
 * --- retrieving/assigning the SelectionKey associated with a channel
 * --- retrieving/assigning the associated NioAttachment
 * --- management of the SelectionKey's interestOps based on the NioDispatchSource's interestOps
 *
 * The Selector's nio operations are themselves more or less asynchronous, and it appears that a get
 * will not necessarily return the value set by a previously executed set. This is how I interpret
 * calls to selector.selectNow() (between JORAMMQ-BEGIN and JORAMMQ-END), which seem to act as a synchronisation
 * of these operations.
 *
 * Nio operations are thread safe, so they could be called from different threads.
 * However, hawtdispatch chooses to perform those operations always from the same thread, for a given source.
 * This thread is chosen when the NioDispatchSource is created, and it is referred to by the selectorQueue variable.
 * Down calls are forced to be run by this thread by using the source selectorQueue. In order to ensure that upcalls
 * are also run by this thread, there is 1 NioManager created for each thread in hawtdispatch. When the SelectionKey
 * is created, the NioDispatchSource ensures that it is running in its selectorQueue. It will then be registered in
 * the NioManager local to this thread. Conversely, when the NioManager detects ready events from its select call,
 * it can directly upcall the NioDispatchSource as it is running in the proper thread.
 *
 * This separate thread management for nio and hawtdispatch operations of a transport is the underlying reason
 * for the asynchronous transport stop algorithm, as this algorithm involves nio operations.
 *
 * Note that the read and write channels of a transport are managed by separate NioDispatchSource.
 * It seems that the selectorQueues of the two sources may or may not be attached to the same thread.
 */
public class NioManager {

  public static final Logger logger = Logger.getLogger("org.fusesource.hawtdispatch.NioManager");

    /**
     * Set the "hawtdispatch.workaround-select-spin" System property to "true" if your
     * seeing the 100% CPU usage in the Selector.select() call.  This enables a
     * workaround for a JVM/OS bug documented at http://bugs.sun.com/view_bug.do?bug_id=6693490
     */
    final SelectStrategy selectStrategy = Boolean.getBoolean("hawtdispatch.workaround-select-spin") ? new WorkAroundSelectSpin() :  new SelectStrategy();

    /**
     * Handles doing a select on a selector.  Allows us to change
     * the implementation to work around bugs in some JVMs.
     */
    class SelectStrategy {
        public int select(long timeout) throws IOException {
            int rc=0;
            if (timeout == -1) {
                trace("entered blocking select");
                rc = selector.select();
                trace("exited blocking select");
            } else {
                trace("entered blocking select with timeout");
                rc = selector.select(timeout);
                trace("exited blocking select with timeout");
            }
            return rc;
        }
    }

    /**
     * Workaround for the selector spin bug.
     */
    class WorkAroundSelectSpin extends SelectStrategy {
        int spins;

        /**
         * Was a wakeup() issued after we entered the select() ??
         * @return
         */
        public boolean wakeupPending() {
            return selectCounter != wakeupCounter.get();
        }

        /**
         * Detects the buggy condition and works around by
         * re-creating the selector when the bug is triggered.
         */
        @Override
        public int select(long timeout) throws IOException {

            if( selector.keys().isEmpty() || ( timeout > 0 && timeout < 100) ) {
                // we can't detect spin in this case
                return super.select(timeout);
            } else {

                long start = System.nanoTime();
                int selected = super.select(timeout);

                // Did the select return immediately with 0 selections?
                if (selected == 0 && !wakeupPending() ) {
                    long end = System.nanoTime();
                    long duration = TimeUnit.NANOSECONDS.toMillis(end-start);
                    if( duration < 50 ) {
                        spins++;
                        if(spins > 10) {
                            reset();
                            spins=0;
                        }
                    } else {
                        spins=0; // not spinning... reset the spin counter
                    }
                } else {
                    spins=0; // not spinning... reset the spin counter
                }
                return selected;
            }
        }

        /**
         * Called when the buggy condition is detected.
         */
        private void reset() throws IOException {
            trace("Selector spin detected... resetting the selector");
            Selector nextSelector = Selector.open();
            for (SelectionKey key : selector.keys()) {
                NioAttachment attachment = (NioAttachment) key.attachment();
                if( key.isValid() ) {
                    try {
                        SelectionKey nextKey = key.channel().register(nextSelector, key.interestOps());
                        attachment.key = nextKey;
                        nextKey.attach(attachment);
                    } catch (IOException e ) {
                        // channel could have closed out
                        cancel(key);
                    }
                } else {
                    // perhaps key was canceled.
                    cancel(key);
                }
            }
            // Close out the old selector and set it to the new one.
            selector.close();
            selector = nextSelector;
        }
    }


    private Selector selector;
    final protected AtomicInteger wakeupCounter = new AtomicInteger();
    volatile protected int selectCounter;
    private final AtomicInteger registeredKeys = new AtomicInteger();

    volatile protected boolean selecting;

    // reproduce the multi step shutdown from HawtDispatcher
    int shutdownState = 0;

    public NioManager() throws IOException {
        this.selector = Selector.open();
    }

    /**
     * @return true if the selector was selecting..
     */
    public boolean wakeupIfSelecting() {
        if( wakeupCounter.getAndIncrement() == selectCounter && selecting) {
            selector.wakeup();
            return true;
        }
        return false;
    }

    /**
     * Selects ready sources, potentially blocking. If wakeup is called during
     * select the method will return.
     *
     * @param timeout
     *            A negative value cause the select to block until a source is
     *            ready, 0 will do a non blocking select. Otherwise the select
     *            will block up to timeout in milliseconds waiting for a source
     *            to become ready.
     * @throws IOException
     */
    public int select(long timeout) throws IOException {
        try {
            if (timeout == 0) {
                selector.selectNow();
            } else {
                selecting=true;
                try {
                    if( selectCounter == wakeupCounter.get()) {
                        selectStrategy.select(timeout);
                    } else {
                        selector.selectNow();
                    }
                } finally {
                    selecting=false;
                    selectCounter = wakeupCounter.get();
                }
            }
        } catch (CancelledKeyException e) {
        }
        return processSelected();
    }

    private int processSelected() {

        if( selector.keys().isEmpty() ) {
            return 0;
        }

        Set<SelectionKey> selectedKeys = selector.selectedKeys();
        int size = selectedKeys.size();
        if (size!=0) {
            trace("selected: %d",size);

            // Copy the key set.. to avoid getting ConcurrentModificationException
            // as it may get changed once we start processing the IO events.
            ArrayList<SelectionKey> copy = new ArrayList<SelectionKey>(selector.selectedKeys());
            selector.selectedKeys().clear();

            // Walk the set of ready keys servicing each ready context:
            for (SelectionKey key : copy) {
                if (key.isValid()) {
                    try {
                        key.interestOps(key.interestOps() & ~key.readyOps());
                        ((NioAttachment) key.attachment()).selected(key);
                    } catch (CancelledKeyException e) {
                        cancel(key);
                    }
                } else {
                    cancel(key);
                }
            }
        }
        return size;
    }

    // this function never seems to be called
    public void shutdown() throws IOException {
        for (SelectionKey key : selector.keys()) {
            NioDispatchSource source = (NioDispatchSource) key.attachment();
            source.cancel();
        }
        selector.close();
    }

    public void shutdown(int level) throws IOException {
      while (shutdownState < level) {
        shutdownState ++;
        switch (shutdownState) {
        case 1:
          for (SelectionKey key : selector.keys()) {
            NioDispatchSource source = (NioDispatchSource) key.attachment();
            source.cancel();
          }
          break;
        case 2:
          selector.close();
          if (TRACE) {
            logger.fine("final NioManager traces: " + traces.size());
            for (String msg: traces)
              logger.fine(msg);
          }
          break;
        }
      }
    }

    private final boolean TRACE = false;
    private final LinkedList<String> traces = new LinkedList<String>();
    protected void trace(String str, Object... args) {
        if (TRACE) {
            String msg = format("%1$tT.%1$tL | %2$s | %3$s",
                                System.currentTimeMillis(), Thread.currentThread().getName(),
                                format(str, args));
            synchronized(traces) {
                traces.add(msg);
                if( traces.size() > 100 ) {
                    traces.removeFirst();
                }
            }
        }
    }

    public NioAttachment register(SelectableChannel channel, int interestOps) throws ClosedChannelException {

      if (shutdownState >= 1)
        throw new IllegalStateException("NioManager shutting down");

      SelectionKey key = channel.keyFor(selector);

      // JORAMMQ-BEGIN
      if (key != null && ! key.isValid()) {
        try {
          selector.selectNow();
          key = channel.keyFor(selector);
        } catch (Exception ignore) {}
      }
      // Note SL: I believe this code should be removed. Here is my interpretation
      // Javadoc says: A key is valid upon creation and remains so until it is cancelled,
      // its channel is closed, or its selector is closed. 
      // If the channel is closed or the selector is closed, then there is no sense in trying to register again.
      // The last case, the key itself being cancelled, cannot occur asynchronously. In the hawtdispatch framework,
      // a SelectionKey is operated in the thread of the selectorQueue only, which is currently executing.
      // If it has been cancelled, then it must be an explicit cancel from the other (read vs write) source,
      // which occured before this current call to register. A call to selectNow will not change the result
      // of the next call to channel.keyFor.
      // Moreover a cancel of the key should lead to a stop of the associated transport.
      // We should not try to register a new key.
      // 
      // I leave the code for now. Has it been added to circumvent a bug in the jdk?
      // JORAMMQ-END

      if( key==null ) {
        try {
          // Note SL: this code is safe, as the nio operations are executed in a single thread.
          // If the selectorQueues of the read and write sources operate in the same thread, then
          // the SelectionKey is shared but cannot be modified concurrently.
          // If they operate in separate threads, then they use their own NioManager and Selector,
          // and their own separate SelectionKey.
          key = channel.register(selector, interestOps);
          key.attach(new NioAttachment(key));
          registeredKeys.incrementAndGet();
        } catch (Exception e) {
          // clean what must be cleaned before forwarding the exception
          if (key != null) {
            // this should not occur, the only sensible failure point is the register call
            key.cancel();
          }
          throw e;
        }
      } else {
        // the channel.register call in the previous part of the if statement already sets the interestOps
        // of the key; it is useless and costly (synchronized operation) to fix it again
        try {
          // the key could be canceled by now..
          key.interestOps(key.interestOps()|interestOps);
          // Note SL: the 2 interestOps operations above, read and write, are thread safe and implement
          // some kind of synchronization. However they are separate and another thread could theoretically
          // change the value in between. This cannot occur in the hawtdispatch framework, where the key
          // is operated by a single thread.
        } catch (RuntimeException e) {
          // the expected exception is CancelledKeyException, however we want to make sure that
          // all exceptions are caught
          // the cancel algorithm fails at this point, as the NioDispatchSource has not yet
          // completed the register_on call.
          // However the other source has already registered and must be cleaned
          cancel(key);
          throw e;
        }
      }
      return (NioAttachment)key.attachment();
    }

    public int getRegisteredKeyCount() {
        return registeredKeys.get();
    }

    /**
     * Cleans all operations related to the SelectionKey.
     * This includes canceling the nio key, and freeing the NioAttachment, and removing all related links
     * in the NioManager.
     * This function should be used as the handler for any unexpected exception during the use of the key.
     * 
     * @param key the nio SelectionKey to clean
     */
    public void cancel(SelectionKey key) {
      NioAttachment attachment = (NioAttachment) key.attachment();
      if( attachment!=null ) {
        // stop any nio upcall
        key.attach(null);
        // this call leads to a recursive call to cancel:
        // NioAttachment.cancel
        //   NioDispatchSource.internal_cancel
        //     NioDispatchSource.key_cancel
        //       NioManager.cancel
        //         -> exits the recursion because key.attachment returns null
        attachment.cancel();
        key.cancel();

        // JORAMMQ-BEGIN This code is commented in 1.20-JoramMQ-1
        try {
          // To make sure the key is canceled out now.
          selector.selectNow();
        } catch (Exception ignore) {
        }
        // JORAMMQ-END

        registeredKeys.decrementAndGet();
      }
    }


}
