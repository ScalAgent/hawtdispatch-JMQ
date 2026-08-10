/**
 * Copyright (C) 2010, FuseSource Corp.  All rights reserved.
 * Copyright (C) 2026 ScalAgent D.T
 */
package org.fusesource.hawtdispatch;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 *  We prefer the use of Task over Runnable since the
 *  JVM can more efficiently invoke methods of
 *  an abstract class than a interface.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class Task implements Runnable {

  public static final Logger taskLogger = Logger.getLogger("org.fusesource.hawtdispatch.Task");
  public static final boolean DEBUG_TASK  = taskLogger.isLoggable(Level.FINE);

  // name used in traces
  final String name;

  @Override
  abstract public void run();

  /**
   * Constructor setting name.
   *
   * @param name  task name used in traces
   */
  public Task(String name) {
    this.name = (name == null ? null : "Task[" + name + "]");
  }

  /**
   * Default constructor for backward compatibility
   */
  public Task() {
    name=null;
  }

  @Override
  public String toString() {
    if (name != null)
      return name;
    return super.toString();
  }
}
