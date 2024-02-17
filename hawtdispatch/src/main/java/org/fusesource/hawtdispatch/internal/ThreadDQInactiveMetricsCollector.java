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

/**
 * Inactive implementation of ThreadDQMetricsCollector which minimizes the overcost.
 */
final public class ThreadDQInactiveMetricsCollector extends ThreadDQMetricsCollector {

    public static final ThreadDQInactiveMetricsCollector INSTANCE = new ThreadDQInactiveMetricsCollector();

    public Task track(Task runnable) {
        return runnable;
    }

    public Metrics metrics() {
        return null;
    }

    public long setLastGlobalPoll(long now) {
      return now;
    }
    
    public long setLastSourcePoll(long now) {
      return now;
    }
    
    public long setLastSelect(long now) {
      return now;
    }

    public long setParkTime(long now) {
      return now;
    }
    
}
