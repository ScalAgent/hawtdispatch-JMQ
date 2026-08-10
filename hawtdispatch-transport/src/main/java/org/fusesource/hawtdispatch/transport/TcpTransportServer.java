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

package org.fusesource.hawtdispatch.transport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.DispatchSource;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.TaskWrapper;

/**
 * A TCP based implementation of {@link TransportServer}
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

public class TcpTransportServer implements TransportServer {

    protected final String bindScheme;
    protected final InetSocketAddress bindAddress;
    protected int backlog = 100;
    protected ServerSocketChannel channel;
    protected TransportServerListener listener;
    protected DispatchQueue dispatchQueue;
    protected DispatchSource acceptSource;
    protected int receiveBufferSize = 64*1024;
    protected int sendBufferSize = 64*1024;
    protected Executor blockingExecutor;

    public static enum State {
      CREATED("CREATED"), STARTING("STARTING"), STARTED("STARTED"), STOPPING("STOPPING"), STOPPED("STOPPED");
      String label;
      State(String label) {
        this.label = label;
      }
    }
    // this variable is not synchronized and should be accessed from the transportServer dispatchQueue
    protected State state = State.CREATED;

    public TcpTransportServer(URI location) throws UnknownHostException {
        bindScheme = location.getScheme();
        String host = location.getHost();
        host = (host == null || host.length() == 0) ? "::" : host;
        bindAddress = new InetSocketAddress(InetAddress.getByName(host), location.getPort());
    }

    public State getState() {
      return state;
    }
    
    @Override
    public void setTransportServerListener(TransportServerListener listener) {
        this.listener = listener;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
      if (channel == null)
        throw new IllegalStateException("not yet bound");
      return (InetSocketAddress) channel.socket().getLocalSocketAddress();
    }

    @Override
    public DispatchQueue getDispatchQueue() {
        return dispatchQueue;
    }

    @Override
    public void setDispatchQueue(DispatchQueue dispatchQueue) {
        this.dispatchQueue = dispatchQueue;
    }

    @Override
    public void suspend() {
        acceptSource.suspend();
    }

    @Override
    public void resume() {
        acceptSource.resume();
    }

    @Override
    @Deprecated
    public void start(Runnable onCompleted) throws Exception {
        start(new TaskWrapper(onCompleted));
    }
    @Override
    @Deprecated
    public void stop(Runnable onCompleted) throws Exception {
        stop(new TaskWrapper(onCompleted));
    }

    @Override
    public void start(Task onCompleted) throws Exception {
      if (dispatchQueue == null)
        throw new IllegalStateException("TcpTransportServer cannot start with a null dispatchQueue");

      state = State.STARTING;
        try {
            channel = ServerSocketChannel.open();
            channel.configureBlocking(false);
            try {
                channel.socket().setReceiveBufferSize(receiveBufferSize);
            } catch (SocketException ignore) {
            }
            try {
                channel.socket().setReceiveBufferSize(sendBufferSize);
            } catch (SocketException ignore) {
            }
            channel.socket().bind(bindAddress, backlog);
        } catch (IOException e) {
            channel.close();
            state = State.STOPPED;
            throw new IOException("Failed to bind to server socket: " + bindAddress + " due to: " + e);
        }

        acceptSource = Dispatch.createSource(channel, SelectionKey.OP_ACCEPT, dispatchQueue);
        String taskName = Task.DEBUG_TASK ? "accept handler for " + dispatchQueue.getLabel() : null;
        acceptSource.setEventHandler(new Task(taskName) {
          @Override
          public void run() {
            // check that this task is still valid
            if (state != State.STARTED)
              return;
            try {
              SocketChannel client = channel.accept();
              while( client!=null ) {
                try {
                  handleSocket(client);
                } catch (Exception e) {
                  // handleSocket should perform any required cleaning before forwarding the exception
                  client.close();
                  listener.onAcceptError(e);
                }
                client = channel.accept();
              }
            } catch (IOException | RuntimeException e) {
              // exception from the accept call, the server can no longer run
              try {
                stop(Dispatch.NOOP);
              } catch (Exception ignore) {
              }
              // we should warn the user level, however there is no dedicated callback available
              // quick solution is to reuse the onAcceptError callback
              Exception exc = new Exception("Stopping server due to " + e);
              listener.onAcceptError(exc);
            }
          }
        });
        taskName = Task.DEBUG_TASK ? "cancel handler for " + dispatchQueue.getLabel() : null;
        acceptSource.setCancelHandler(new Task(taskName) {
          @Override
          public void run() {
            try {
              // this cancel handler is called after the nio registration has been cleaned
              acceptSource = null;
              closeChannel();
            } catch (RuntimeException ignore) {
            }
            // we should warn the user level, however there is no dedicated callback available
            // quick solution is to reuse the onAcceptError callback
            Exception exc = new Exception("Stopping server due to internal error");
            listener.onAcceptError(exc);
          }
        });
        acceptSource.resume();
        state = State.STARTED;
        if( onCompleted!=null ) {
            dispatchQueue.execute(onCompleted);
        }
    }

    @Override
    public String getBoundAddress() {
      if (channel == null)
        throw new IllegalStateException("not yet bound");
      try {
        return new URI(bindScheme, null, bindAddress.getAddress().getHostAddress(), channel.socket().getLocalPort(), null, null, null).toString();
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void stop(final Task onCompleted) throws Exception {
      if (dispatchQueue == null) {
        // the TcpTransportServer should be in CREATED state, it never started
        // nothing to do
        onCompleted.run();
        return;
      }
      // execute the stop algorithm in the TransportServer dispatch queue
      String taskName = Task.DEBUG_TASK ? "stop transport server for " + dispatchQueue.getLabel() : null;
      final Task stopTask = new Task(taskName) {
        @Override
        public void run() {
          switch (state) {
          case CREATED:
          case STOPPED:
            // nothing to do
            onCompleted.run();
            return;
          case STOPPING:
            // should wait for the complete stop before calling the onCompleted callback
            // should we protect again an infinite loop here? this is really a never occur case ...
            dispatchQueue.executeAfter(100, TimeUnit.MILLISECONDS, this);
            return;
          case STARTING:
          case STARTED:
            break;
          }
          state = State.STOPPING;
          // the start may fail on the bind operation
          // in that case the channel is open, but the acceptSource is null
          if (acceptSource == null || acceptSource.isCanceled()) {
            closeChannel();
            onCompleted.run();
          } else {
            String taskName = Task.DEBUG_TASK ? "accept source cancel handler" : null;
            acceptSource.setCancelHandler(new Task(taskName) {
              @Override
              public void run() {
                acceptSource = null;
                closeChannel();
                onCompleted.run();
              }
            });
            acceptSource.cancel();
            // the stop algorithm continues by the cancel handler executed in the transport dispatch queue
          }
        }
      };
      if (dispatchQueue.isExecuting()) {
        stopTask.run();
      } else {
        dispatchQueue.execute(stopTask);
      }
    }

    /**
     * Closes the channel and switch to the STOPPED state.
     * This function is used only in function stop.
     */
    private final void closeChannel() {
      if (channel != null)
        try {
          channel.close();
        } catch (IOException ignore) {
        }
      channel = null;
      state = State.STOPPED;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    protected final void handleSocket(SocketChannel socket) throws Exception {
      TcpTransport transport = null;
      try {
        transport = createTransport();
        transport.connected(socket);
        listener.onAccept(transport);
      } catch (Exception exc) {
        // clean what needs to be cleaned before forwarding the exception
        // if the exception comes from createTransport or connected, then the necessary cleaning
        // should have been done in the respective functions
        // if the exception comes from the onAccept upcall, then we must make sure the transport is stopped
        // the onAccept exception handler may have called stop itself, but this is not a problem
        if (transport != null && transport._serviceState == ServiceBase.STARTED)
          try {
            transport.stop(Dispatch.NOOP);
          } catch (Exception ignore) {
          }
        throw exc;
      }
    }

    protected TcpTransport createTransport() {
        final TcpTransport rc = new TcpTransport();
        rc.setBlockingExecutor(blockingExecutor);
        rc.setDispatchQueue(dispatchQueue);
        return rc;
        // no cleaning operation is necessary in case of exception
        // the created structures can be collected by the garbage
        // no execution component has been started, no external resource has been reserved
    }

    /**
     * @return pretty print of this
     */
    @Override
    public String toString() {
        return getBoundAddress();
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
        if( channel!=null ) {
            try {
                channel.socket().setReceiveBufferSize(receiveBufferSize);
            } catch (SocketException ignore) {
            }
        }
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        if( channel!=null ) {
            try {
                channel.socket().setReceiveBufferSize(sendBufferSize);
            } catch (SocketException ignore) {
            }
        }
    }

    @Override
    public Executor getBlockingExecutor() {
        return blockingExecutor;
    }

    @Override
    public void setBlockingExecutor(Executor blockingExecutor) {
        this.blockingExecutor = blockingExecutor;
    }
}