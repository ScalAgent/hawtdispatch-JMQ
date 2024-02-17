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

package org.fusesource.hawtdispatch.internal.pool;

import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.internal.NioManager;
import org.fusesource.hawtdispatch.internal.ThreadDispatchQueue;
import org.fusesource.hawtdispatch.internal.WorkerThread;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.String.format;

/**
 */
public class SimpleThread extends WorkerThread {

    private SimplePool pool;
    private ThreadDispatchQueue threadQueue;
    private final NioManager nioManager;

    public SimpleThread(SimplePool pool, int index) throws IOException {
        super(pool.group, pool.name + "-" + (index+1));
        this.pool = pool;
        this.nioManager = new NioManager();
        this.threadQueue = new ThreadDispatchQueue(pool.globalQueue, this);
    }

    @Override
    public ThreadDispatchQueue getDispatchQueue() {
        return threadQueue;
    }
    @Override
    public void unpark() {
        nioManager.wakeupIfSelecting();
    }

    @Override
    public NioManager getNioManager() {
        return nioManager;
    }

    @Override
    public void run() {
        debug("run start");
        try {
            ConcurrentLinkedQueue<Task> sharedQueue = pool.tasks;
            while(!pool.shutdown) {

                Task task = threadQueue.poll();
                long now = 0;
                if( task==null ) {
                    now = threadQueue.setLastGlobalPoll(now);
                    task = sharedQueue.poll();
                    if( task==null ) {
                        now = threadQueue.setLastSourcePoll(now);
                        task = threadQueue.getSourceQueue().poll();
                    }
                }

                if( task == null ) {
                    now = threadQueue.setLastSelect(now);
                    pool.park(this);
                    threadQueue.setParkTime(0);
                } else {
                    task.run();
                }
            }
        } finally {
            debug("run end");
        }
    }

    /**
     * Get the size of the pool shared list of tasks.
     * This function is not very efficient nor precise.
     * It gives a transient value of the list size for monitoring purpose.
     */
    public int getPoolTasksSize() {
      return pool.tasks.size();
    }

    public static final boolean DEBUG = false;
    protected void debug(String str, Object... args) {
        if (DEBUG) {
            System.out.println(format("[DEBUG] SimpleThread %s: %s", getName(), format(str, args)));
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
