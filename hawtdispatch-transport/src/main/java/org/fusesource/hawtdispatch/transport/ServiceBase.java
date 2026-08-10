/**
 * Copyright (C) 2012 FuseSource, Inc.
 * http://fusesource.com
 * Copyright (C) 2024 - 2026 ScalAgent Distributed Technologies
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

package org.fusesource.hawtdispatch.transport;

import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.TaskWrapper;

/**
 * <p>
 * The BaseService provides helpers for dealing async service state.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class ServiceBase {

  // setting up the logs may have a strong impact on performances of hawtdispatch threads
  private static final Logger logger = Logger.getLogger("org.fusesource.hawtdispatch.transport");
  private static final boolean DEBUG  = logger.isLoggable(Level.FINE);
  private static final void trace(String message) {
    if (DEBUG)
      logger.fine(message);
  }

    public static class State {
        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
        public boolean isStarted() {
            return false;
        }
        public boolean isStarting() {
            return false;
        }
    }

    static class CallbackSupport extends State {
        LinkedList<Task> callbacks = new LinkedList<Task>();

        void add(Task r) {
            if (r != null) {
                callbacks.add(r);
            }
        }

        void done() {
            for (Task callback : callbacks) {
                callback.run();
            }
        }
    }

    public static final State CREATED = new State() {
      @Override
      public String toString() {
          return "CREATED";
      }
    };
    public static class STARTING extends CallbackSupport {
        @Override
        public boolean isStarting() {
            return true;
        }
    }
    public static final State STARTED = new State() {
        @Override
        public boolean isStarted() {
            return true;
        }
        @Override
        public String toString() {
            return "STARTED";
        }
    };
    public static class STOPPING extends CallbackSupport {
    }

    public static final State STOPPED = new State() {
      @Override
      public String toString() {
          return "STOPPED";
      }
    };


    protected State _serviceState = CREATED;

    final public void start(final Runnable onCompleted) {
        start(new TaskWrapper(onCompleted));
    }

    final public void start(final Task onCompleted) {
      String taskName = Task.DEBUG_TASK ? "start task for " + getDispatchQueue().getLabel() : null;
      Task startTask = new Task(taskName) {
        @Override
        public void run() {
          trace("start transport, _serviceState=" + _serviceState);
          if (_serviceState == CREATED ||
              _serviceState == STOPPED) {
            final STARTING state = new STARTING();
            state.add(onCompleted);
            _serviceState = state;
            String taskName = Task.DEBUG_TASK ? "_start onCompleted task for " + getDispatchQueue().getLabel() : null;
            _start(new Task(taskName) {
              @Override
              public void run() {
                trace("execute _start onCompleted callback");
                _serviceState = STARTED;
                state.done();
              }
            });
          } else if (_serviceState instanceof STARTING) {
            ((STARTING) _serviceState).add(onCompleted);
          } else if (_serviceState == STARTED) {
            if (onCompleted != null) {
              onCompleted.run();
            }
          } else {
            // only possible state is STOPPING
            if (onCompleted != null) {
              // is it really relevant to execute the completion task in this case?
              // the final state will be STOPPED and not STARTED
              onCompleted.run();
            }
            error("start should not be called from state: " + _serviceState);
          }
        }
      };
      if (getDispatchQueue().isExecuting()) {
        startTask.run();
      } else {
        getDispatchQueue().execute(startTask);
      }
    }

    final public void stop(final Runnable onCompleted) {
        stop(new TaskWrapper(onCompleted));
    }

    final public void stop(final Task onCompleted) {
        String taskName = Task.DEBUG_TASK ? "stop task for " + getDispatchQueue().getLabel() : null;
        Task stopTask = new Task(taskName) {
            @Override
            public void run() {
              trace("stop transport, _serviceState=" + _serviceState);
                if (_serviceState == STARTED) {
                    final STOPPING state = new STOPPING();
                    state.add(onCompleted);
                    _serviceState = state;
                    String taskName = Task.DEBUG_TASK ? "_stop onCompleted task for " + getDispatchQueue().getLabel() : null;
                    _stop(new Task(taskName) {
                        @Override
                        public void run() {
                          trace("execute _stop onCompleted callback");
                            _serviceState = STOPPED;
                            state.done();
                        }
                    });
                } else if (_serviceState instanceof STOPPING) {
                    ((STOPPING) _serviceState).add(onCompleted);
                } else if (_serviceState == STOPPED) {
                    if (onCompleted != null) {
                        onCompleted.run();
                    }
                } else {
                    if (onCompleted != null) {
                        onCompleted.run();
                    }
                    error("stop should not be called from state: " + _serviceState);
                }
            }
        };
        if (getDispatchQueue().isExecuting()) {
          stopTask.run();
        } else {
          getDispatchQueue().execute(stopTask);
        }
    }

    private void error(String msg) {
        try {
            throw new AssertionError(msg);
        } catch (AssertionError e) {
            logger.warning(e.getMessage());
            e.printStackTrace();
        }
    }

    final protected State getServiceState() {
        return _serviceState;
    }

    abstract protected DispatchQueue getDispatchQueue();

    abstract protected void _start(Task onCompleted);

    abstract protected void _stop(Task onCompleted);

}