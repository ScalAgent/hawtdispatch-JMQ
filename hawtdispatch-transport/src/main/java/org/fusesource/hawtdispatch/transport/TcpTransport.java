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

package org.fusesource.hawtdispatch.transport;

import static java.lang.String.format;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.fusesource.hawtdispatch.CustomDispatchSource;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.DispatchSource;
import org.fusesource.hawtdispatch.EventAggregators;
import org.fusesource.hawtdispatch.Retained;
import org.fusesource.hawtdispatch.Task;

/**
 * An implementation of the {@link org.fusesource.hawtdispatch.transport.Transport} interface using raw tcp/ip
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TcpTransport extends ServiceBase implements Transport {

    static InetAddress localhost;
    synchronized static public InetAddress getLocalHost() throws UnknownHostException {
        // cache it...
        if( localhost==null ) {
            // this can be slow on some systems and we use repeatedly.
            localhost = InetAddress.getLocalHost();
        }
        return localhost;
    }

    abstract static class SocketState {
        void onStop(Task onCompleted) {
        }
        void onCanceled() {
        }
        boolean is(Class<? extends SocketState> clazz) {
            return getClass()==clazz;
        }
        @Override
        public String toString() {
          return getClass().getSimpleName();
        }
    }

    static class DISCONNECTED extends SocketState{}

    class CONNECTING extends SocketState{
        @Override
        void onStop(Task onCompleted) {
            trace("CONNECTING.onStop");
            // the SocketChannel is open, but the bind failed or the connect has not completed
            // a non blocking connect may has been executed, but the finishConnect has not completed
            // it may be necessary to execute the asynchronous cancel algorithm
            if (readSource != null) {
              CANCELING state = new CANCELING();
              socketState = state;
              state.onStop(onCompleted);
            } else {
              try {
                if( closeOnCancel ) {
                  channel.close();
                }
              } catch (IOException ignore) {
              }
              socketState = new CANCELED(true);
              trace("CONNECTING: run task " + onCompleted);
              onCompleted.run();
            }
        }
        @Override
        void onCanceled() {
            trace("CONNECTING.onCanceled");
            CANCELING state = new CANCELING();
            socketState = state;
            state.onCanceled();
        }
    }

    class CONNECTED extends SocketState {

        public CONNECTED() {
            localAddress = channel.socket().getLocalSocketAddress();
            remoteAddress = channel.socket().getRemoteSocketAddress();
        }

        @Override
        void onStop(Task onCompleted) {
            trace("CONNECTED.onStop");
            if (readSource != null || writeSource != null) {
              // standard case, need nio cleaning
              // start the asynchronous cancel algorithm
              CANCELING state = new CANCELING();
              socketState = state;
              state.add(createDisconnectTask());
              state.onStop(onCompleted);
              // the algorithm continues with the CANCELING.onCanceled callbacks
            } else {
              // should probably never occur in the CONNECTED state of a client transport,
              // as the onConnected call immediately follows the CONNECTED state and
              // sets the readSource and the writeSource
              // however it may occur for a session transport which is in the CONNECTED state
              // when the onAccept upcall is executed. In that case there is no listener set.
              try {
                if( closeOnCancel ) {
                  channel.close();
                }
              } catch (IOException ignore) {
              }
              if (listener != null)
                listener.onTransportDisconnected();
              socketState = new CANCELED(true);
              trace("CONNECTED: run task " + onCompleted);
              onCompleted.run();
            }
        }
        @Override
        void onCanceled() {
            trace("CONNECTED.onCanceled");
            CANCELING state = new CANCELING();
            socketState = state;
            state.add(createDisconnectTask());
            state.onCanceled();
        }
        Task createDisconnectTask() {
            String taskName = Task.DEBUG_TASK ? "disconnect task " + (dispatchQueue == null ? "" : dispatchQueue.getLabel()) : null;
            return new Task(taskName) {
                @Override
                public void run() {
                    listener.onTransportDisconnected();
                }
            };
        }
    }

    class CANCELING extends SocketState {
        private LinkedList<Task> runnables =  new LinkedList<Task>();
        private int remaining;
        private boolean dispose;

        public CANCELING() {
            if( readSource!=null ) {
                remaining++;
                readSource.cancel();
                // it might be useful to nullify readSource, otherwise a later dispose will call cancel again unnecessarily
                // readSource = null;
            }
            if( writeSource!=null ) {
                remaining++;
                writeSource.cancel();
                // it might be necessary to nullify writeSource, otherwise a later dispose will call cancel again unnecessarily
                // writeSource = null;
            }
        }
        @Override
        void onStop(Task onCompleted) {
            trace("CANCELING.onStop");
            add(onCompleted);
            // Note SL: I cannot see the interest of setting dispose to true;  it will trigger a call to dispose
            // in onCanceled, which will call again the cancels already called in the constructor
            dispose = true;
        }
        void add(Task onCompleted) {
            if( onCompleted!=null ) {
                runnables.add(onCompleted);
            }
        }
        @Override
        void onCanceled() {
            trace("CANCELING.onCanceled");
            remaining--;
            if( remaining!=0 ) {
                return;
            }
            try {
                if( closeOnCancel ) {
                    channel.close();
                }
            } catch (IOException ignore) {
            }
            // Note SL: I would call new CANCELED(true) here
            // if dispose is false, then CANCELED.onStop will call dispose, which will call again the cancels
            // already called in the constructor of CANCELING
            socketState = new CANCELED(dispose);
            for (Task runnable : runnables) {
              trace("CANCELING: run task " + runnable);
              runnable.run();
            }
            if (dispose) {
                dispose();
            }
        }
    }

    class CANCELED extends SocketState {
        private boolean disposed;

        public CANCELED(boolean disposed) {
            this.disposed=disposed;
        }

        @Override
        void onStop(Task onCompleted) {
            trace("CANCELED.onStop");
            if( !disposed ) {
                disposed = true;
                dispose();
            }
            trace("CANCELED: run task " + onCompleted);
            onCompleted.run();
        }
    }

    protected URI remoteLocation;
    protected URI localLocation;
    protected TransportListener listener;
    protected ProtocolCodec codec;

    protected SocketChannel channel;

    protected SocketState socketState = new DISCONNECTED();

    protected DispatchQueue dispatchQueue;
    private DispatchSource readSource;
    private DispatchSource writeSource;
    protected CustomDispatchSource<Integer, Integer> drainOutboundSource;
    protected CustomDispatchSource<Integer, Integer> yieldSource;

    protected boolean useLocalHost = true;

    int maxReadRate;
    int maxWriteRate;
    int receiveBufferSize = 1024*64;
    int sendBufferSize = 1024*64;
    boolean closeOnCancel = true;

    boolean keepAlive = true;

    public static final int IPTOS_LOWCOST = 0x02;
    public static final int IPTOS_RELIABILITY = 0x04;
    public static final int IPTOS_THROUGHPUT = 0x08;
    public static final int IPTOS_LOWDELAY = 0x10;

    int trafficClass = IPTOS_THROUGHPUT;

    protected RateLimitingChannel rateLimitingChannel;
    SocketAddress localAddress;
    SocketAddress remoteAddress;
    protected Executor blockingExecutor;

    class RateLimitingChannel implements ScatteringByteChannel, GatheringByteChannel {

        int read_allowance = maxReadRate;
        boolean read_suspended = false;
//        int read_resume_counter = 0;
        int write_allowance = maxWriteRate;
        boolean write_suspended = false;

        public void resetAllowance() {
            if( read_allowance != maxReadRate || write_allowance != maxWriteRate) {
                read_allowance = maxReadRate;
                write_allowance = maxWriteRate;
                if( write_suspended ) {
                    write_suspended = false;
                    resumeWrite();
                }
                if( read_suspended ) {
                    read_suspended = false;
                    resumeRead();
                }
            }
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if( maxReadRate ==0 ) {
                return channel.read(dst);
            } else {
                int rc=0;
                int reduction = 0;
                try {
                    int remaining = dst.remaining();
                    if( read_allowance ==0 || remaining ==0 ) {
                        return 0;
                    }
                    if( remaining > read_allowance) {
                        reduction = remaining - read_allowance;
                        ((java.nio.Buffer) dst).limit(dst.limit() - reduction);
                    }
                    rc = channel.read(dst);
                    read_allowance -= rc;
                } finally {
                    if( read_allowance<=0 && !read_suspended ) {
                        // we need to suspend the read now until we get
                        // a new allowance..
                        readSource.suspend();
                        read_suspended = true;
                    }
                    if( reduction!=0 ) {
                        dst.limit(dst.limit() + reduction);
                    }
                }
                return rc;
            }
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if( maxWriteRate ==0 ) {
                return channel.write(src);
            } else {
                int remaining = src.remaining();
                if( write_allowance ==0 || remaining ==0 ) {
                    return 0;
                }

                int reduction = 0;
                if( remaining > write_allowance) {
                    reduction = remaining - write_allowance;
                    src.limit(src.limit() - reduction);
                }
                int rc = 0;
                try {
                    rc = channel.write(src);
                    write_allowance -= rc;
                } finally {
                    if( reduction!=0 ) {
                        if( src.remaining() == 0 ) {
                            // we need to suspend the read now until we get
                            // a new allowance..
                            write_suspended = true;
                            suspendWrite();
                        }
                        src.limit(src.limit() + reduction);
                    }
                }
                return rc;
            }
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        public void resumeRead() {
//            if( read_suspended ) {
//                read_resume_counter += 1;
//            } else {
                _resumeRead();
//            }
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            if(offset+length > dsts.length || length<0 || offset<0) {
                throw new IndexOutOfBoundsException();
            }
            long rc=0;
            for (int i = 0; i < length; i++) {
                ByteBuffer dst = dsts[offset+i];
                if(dst.hasRemaining()) {
                    rc += read(dst);
                }
                if( dst.hasRemaining() ) {
                    return rc;
                }
            }
            return rc;
        }

        @Override
        public long read(ByteBuffer[] dsts) throws IOException {
            return read(dsts, 0, dsts.length);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            if(offset+length > srcs.length || length<0 || offset<0) {
                throw new IndexOutOfBoundsException();
            }
            long rc=0;
            for (int i = 0; i < length; i++) {
                ByteBuffer src = srcs[offset+i];
                if(src.hasRemaining()) {
                    rc += write(src);
                }
                if( src.hasRemaining() ) {
                    return rc;
                }
            }
            return rc;
        }

        @Override
        public long write(ByteBuffer[] srcs) throws IOException {
            return write(srcs, 0, srcs.length);
        }

    }

    private final Task CANCEL_HANDLER = new Task("CANCEL_HANDLER") {
        @Override
        public void run() {
          trace("CANCEL_HANDLER");
          socketState.onCanceled();
        }
    };

    static final class OneWay {
        final Object command;
        final Retained retained;

        public OneWay(Object command, Retained retained) {
            this.command = command;
            this.retained = retained;
        }
    }

    public void connected(SocketChannel channel) throws IOException, Exception {
        this.channel = channel;
        initializeChannel();
        this.socketState = new CONNECTED();
    }

    protected void initializeChannel() throws Exception {
        this.channel.configureBlocking(false);
        Socket socket = channel.socket();
        try {
            socket.setReuseAddress(true);
        } catch (SocketException e) {
        }
        try {
            socket.setSoLinger(true, 0);
        } catch (SocketException e) {
        }
        try {
            socket.setTrafficClass(trafficClass);
        } catch (SocketException e) {
        }
        try {
            socket.setKeepAlive(keepAlive);
        } catch (SocketException e) {
        }
        try {
            socket.setTcpNoDelay(true);
        } catch (SocketException e) {
        }
        try {
            socket.setReceiveBufferSize(receiveBufferSize);
        } catch (SocketException e) {
        }
        try {
            socket.setSendBufferSize(sendBufferSize);
        } catch (SocketException e) {
        }

        if( channel!=null && codec!=null ) {
            initializeCodec();
        }

        // no cleaning operation is necessary in case of exception
        // the created structures can be collected by the garbage
        // no execution component has been started, no external resource has been reserved
    }

    protected void initializeCodec() throws Exception {
        codec.setTransport(this);
    }

    private void initRateLimitingChannel() {
        if( (maxReadRate !=0 || maxWriteRate !=0) && rateLimitingChannel==null ) {
            rateLimitingChannel = new RateLimitingChannel();
        }
    }

    public void connecting(final URI remoteLocation, final URI localLocation) throws Exception {
        this.channel = SocketChannel.open();
        initializeChannel();
        this.remoteLocation = remoteLocation;
        this.localLocation = localLocation;
        socketState = new CONNECTING();
    }


    @Override
    public DispatchQueue getDispatchQueue() {
        return dispatchQueue;
    }

    @Override
    public void setDispatchQueue(DispatchQueue queue) {
        this.dispatchQueue = queue;
        if(readSource!=null) readSource.setTargetQueue(queue);
        if(writeSource!=null) writeSource.setTargetQueue(queue);
        if(drainOutboundSource!=null) drainOutboundSource.setTargetQueue(queue);
        if(yieldSource!=null) yieldSource.setTargetQueue(queue);
    }

    // ==================================================
    // Modification for use HTTP CONNECT # BEGIN
    // ==================================================

    private Proxy proxy = null;
    private String auth = null;

    public final Proxy getProxy() {
      return proxy;
    }

    public final void setProxy(String host, int port, String auth) {
      this.proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(host, port));
      this.auth = auth;
    }

    private boolean connect(SocketChannel channel, InetSocketAddress remoteAddr) throws IOException {
      boolean success = false;
      ByteBuffer proxyConnect = null;
      SocketAddress sa;

      trace("connect: " + proxy);
      boolean blocking = channel.isBlocking();
      trace("connect: blocking=" + blocking);
      if (proxy == null) {
        sa = remoteAddr;
      } else {
        sa = proxy.address();
        proxyConnect = createProxyRequest(remoteAddr.getHostString(), remoteAddr.getPort(), auth);
        try {
          channel.configureBlocking(true);
        } catch (IOException ioe) {
          throw ioe;
        }
      }

      // Get the connection timeout
      long timeout = 1000;

      try {
        // Open the connection
        trace("connect: connecting...");
        success = channel.connect(sa);
        trace("connect: connected -> " + success);

        if (proxyConnect != null) {
          ByteBuffer response = ByteBuffer.allocate(4096);

          // Proxy CONNECT is clear text
          //          channel = new ChannelWrapperNonSecure(socketChannel);
          writeRequest(channel, proxyConnect, timeout);
          trace("connect: writeRequest ok");
          int statusCode = processResponse(response, channel, timeout);
          trace("connect: processResponse ok");
          if (statusCode != 200) {
            trace("connect: statusCode=" + statusCode);
            throw new Exception("TcpTransport.connect:" + Integer.toString(statusCode));
          }
        }

        return success;
      } catch (Exception e) {
        throw new IOException(e);
      } finally {
        if (success && proxy != null) {
          try {
            channel.configureBlocking(blocking);
            trace("connect: configureBlocking -> " + blocking);
          } catch (IOException ioe) {

          }
        }
      }
    }

    private ByteBuffer createProxyRequest(String host, int port, String auth) {
      StringBuilder request = new StringBuilder();
      request.append("CONNECT ");
      request.append(host);
      request.append(':');
      request.append(port);

      request.append(" HTTP/1.1\r\nProxy-Connection: keep-alive\r\nConnection: keepalive\r\nHost: ");
      request.append(host);
      request.append(':');
      request.append(port);
      if (auth != null)
        request.append("\r\nProxy-Authorization: basic ").append(auth);
      request.append("\r\n\r\n");

      byte[] bytes = request.toString().getBytes(StandardCharsets.ISO_8859_1);
      return ByteBuffer.wrap(bytes);
    }

    private void writeRequest(SocketChannel channel, ByteBuffer request, long timeout) throws Exception {
      int toWrite = request.limit();

      int thisWrite = channel.write(request);
      toWrite -= thisWrite;

      while (toWrite > 0) {
        thisWrite = channel.write(request);
        toWrite -= thisWrite;
      }
    }

    private String readLine(ByteBuffer response) {
      // All ISO-8859-1
      StringBuilder sb = new StringBuilder();

      char c = 0;
      while (response.hasRemaining()) {
        c = (char) response.get();
        sb.append(c);
        if (c == 10) {
          break;
        }
      }

      return sb.toString();
    }

    private void parseHeaders(String line, Map<String,List<String>> headers) {
      // Treat headers as single values by default.

      int index = line.indexOf(':');
      if (index == -1) {
        //          log.warn(sm.getString("wsWebSocketContainer.invalidHeader", line));
        return;
      }
      // Header names are case insensitive so always use lower case
      String headerName = line.substring(0, index).trim().toLowerCase(Locale.ENGLISH);
      // Multi-value headers are stored as a single header and the client is
      // expected to handle splitting into individual values
      String headerValue = line.substring(index + 1).trim();

      List<String> values = headers.get(headerName);
      if (values == null) {
        values = new ArrayList<>(1);
        headers.put(headerName, values);
      }
      values.add(headerValue);
    }

    private int parseStatus(String line) throws Exception {
      // This client only understands HTTP 1.
      // RFC2616 is case specific
      String[] parts = line.trim().split(" ");
      // CONNECT for proxy may return a 1.0 response
      if (parts.length < 2 || !("HTTP/1.0".equals(parts[0]) || "HTTP/1.1".equals(parts[0]))) {
        throw new Exception("wsWebSocketContainer.invalidStatus: " + line);
      }
      try {
        return Integer.parseInt(parts[1]);
      } catch (NumberFormatException nfe) {
        throw new Exception("wsWebSocketContainer.invalidStatus: " + line);
      }
    }

    private int processResponse(ByteBuffer response, SocketChannel channel, long timeout) throws Exception {
      Map<String,List<String>> headers = new HashMap<String,List<String>>();

      int status = 0;
      boolean readStatus = false;
      boolean readHeaders = false;
      String line = null;
      while (!readHeaders) {
        // On entering loop buffer will be empty and at the start of a new
        // loop the buffer will have been fully read.
        ((java.nio.Buffer) response).clear();
        // Blocking read
        int bytesRead = channel.read(response);
        if (bytesRead == -1) {
          throw new EOFException("wsWebSocketContainer.responseFail: " + Integer.toString(status));
        }
        ((java.nio.Buffer) response).flip();
        while (response.hasRemaining() && !readHeaders) {
          if (line == null) {
            line = readLine(response);
          } else {
            line += readLine(response);
          }
          if ("\r\n".equals(line)) {
            readHeaders = true;
          } else if (line.endsWith("\r\n")) {
            if (readStatus) {
              parseHeaders(line, headers);
            } else {
              status = parseStatus(line);
              readStatus = true;
            }
            line = null;
          }
        }
      }

      return status;
    }

    // ==================================================
    // Modification for use HTTP CONNECT # END
    // ==================================================

    @Override
    public void _start(Task onCompleted) {
      trace("_start from " + getRemoteAddress() + ", socketState=" + socketState);
      try {
        if (socketState.is(CONNECTING.class)) {

          // Resolving host names might block.. so do it on the blocking executor.
          this.blockingExecutor.execute(new Runnable() {
            @Override
            public void run() {
              try {

                final InetSocketAddress localAddress = (localLocation != null)
                    ? new InetSocketAddress(InetAddress.getByName(localLocation.getHost()), localLocation.getPort())
                    : null;

                String host = resolveHostName(remoteLocation.getHost());
                final InetSocketAddress remoteAddress = new InetSocketAddress(host, remoteLocation.getPort());

                // Done resolving.. switch back to the dispatch queue.
                String taskName = Task.DEBUG_TASK ? "_start main for " + dispatchQueue.getLabel() : null;
                dispatchQueue.execute(new Task(taskName) {
                  @Override
                  public void run() {
                    trace("continue _start: socketState=" + socketState);
                    // check that this task is still valid
                    // No need to complete if we have been canceled.
                    if(getServiceState() != STARTED || ! socketState.is(CONNECTING.class) ) {
                      trace("_start interrupted.");
                      return;
                    }
                    // _start main step
                    try {

                      if (localAddress != null) {
                        channel.socket().bind(localAddress);
                      }
                      trace("connecting...");
                      if (connect(channel, remoteAddress)) {
//                    if (channel.connect(remoteAddress)) {
                        trace("connected");
                        socketState = new CONNECTED();
                        onConnected();
                        return;
                      }

                      // The asynchronous connect code has been reactivated

                      // this allows the connect to complete..
                      readSource = Dispatch.createSource(channel, SelectionKey.OP_CONNECT, dispatchQueue);
                      String taskName = Task.DEBUG_TASK ? "async connect for " + dispatchQueue.getLabel() : null;
                      readSource.setEventHandler(new Task(taskName) {
                        @Override
                        public void run() {
                          // check that this task is still valid
                          if (getServiceState() != STARTED || ! socketState.is(CONNECTING.class)) {
                            trace("asynchronous connect dropped.");
                            return;
                          }
                          try {
                            channel.finishConnect();
                            readSource.setCancelHandler(null);
                            readSource.cancel();
                            readSource = null;
                            trace("connected.");
                            socketState = new CONNECTED();
                            onConnected();
                          } catch (IOException | RuntimeException e) {
                            trace("_start async connect: call onTransportFailure, socketState=" + socketState);
                            onTransportFailure(e);
                            // make sure the transport will not freeze in case of RuntimeException
                            // TcpTransport.onConnected actually raises no exception
                            // However the SslTransport implementation does raise an exception
                            // - in case of write error in the channel
                            // - in case of channel closed
                          }
                        }
                      });
                      readSource.setCancelHandler(CANCEL_HANDLER);
                      readSource.resume();

                      // the onCompleted callback should be executed here instead of in the finally clause
                      // however this would leave the transport in a STARTING state which should be handled
                      // in the error handling code
                    } catch (IOException | RuntimeException e) {
                      // it is theoretically possible that the exception is raised from the onConnected call
                      // the read/write sources could have been created
                      // it is also possible that the call to bind failed, and the socket is not even connected
                      /*
                       * simplify the exception handler
                      if (readSource != null || writeSource != null) {
                        // standard case, need nio cleaning
                        // start the asynchronous cancel algorithm
                        socketState = new CANCELING();
                        // the algorithm continues with the CANCELING.onCanceled callbacks
                        // we could add an onCompleted callback, however we choose to keep the original code
                        // which calls listener.onTransportFailure, triggering a transport.stop
                      } else {
                        try {
                          if( closeOnCancel ) {
                            channel.close();
                          }
                        } catch (IOException ignore) {
                        }
                        socketState = new CANCELED(true);
                      }
                      */
                      trace("_start: call onTransportFailure, socketState=" + socketState);
                      onTransportFailure(e);
                    }
                  }
                });

              } catch (final IOException | RuntimeException e) {
                // we're in blockingExecutor thread context here
                // the _start main step has not begun and will not execute
                String taskName = Task.DEBUG_TASK ? "Exception handler for " + dispatchQueue.getLabel() : null;
                dispatchQueue.execute(new Task(taskName) {
                  @Override
                  public void run() {
                    // make sure the task is still valid, a stop may have been called in between
                    if (_serviceState != STARTED)
                      return;
                    /*
                     * simplify the exception handler
                    try {
                      channel.close();
                    } catch (IOException | RuntimeException ignore) {
                    }
                    // we can safely switch to the terminal state (of the TcpTransport)
                    // beware that the ServiceBase state will be set to STARTED in the finally clause
                    socketState = new CANCELED(true);
                    */
                    trace("_start DNS task: call onTransportFailure, socketState=" + socketState);
                    onTransportFailure(e);
                  }
                });
              }
            }
          });
        } else if (socketState.is(CONNECTED.class)) {
          // this case comes from connected(SocketChannel), called by a TcpTransportServer
          // the connection has already been accepted
          String taskName = Task.DEBUG_TASK ? "_start CONNECTED case for " + dispatchQueue.getLabel() : null;
          dispatchQueue.execute(new Task(taskName) {
            // queue switch probably useless, as _start is already executed in the transport queue
            // it has probably been added so that the finally clause (serviceState=STARTED)
            // is executed before onConnected as this is the case for a client side TcpTransport
            @Override
            public void run() {
              // make sure the task is still valid
              if (_serviceState != STARTED || !socketState.is(CONNECTED.class)) {
                trace("Session transport connection dropped.");
                return;
              }
              try {
                trace("was connected.");
                onConnected();
              } catch (IOException | RuntimeException e) {
                onTransportFailure(e);
              }
            }
          });
        } else if (socketState.is(CANCELING.class) || socketState.is(CANCELED.class)) {
          // assume that the transport has been stopped
          // should never occur
          logger.info("starting a transport with socketState " + socketState);
        } else {
          // other hypothetically possible states are DISCONNECTED
          // DISCONNECTED requires a connecting call before start
          logger.warning("transport cannot be started,  socket state is: " + socketState);
        }
      } finally {
        // NOTE: this code is executed before the actual start algorithm executes, as it executes
        // asynchronously in the blockingExecutor for a client transport, or with a Task indirection
        // for a sessions transport
        // this leads to the _serviceState switching to STARTED while it is not actually started,
        // and to a probably useless and probably unused onCompleted upcall.
        trace("_start: finally clause, socketState=" + socketState + ", onCompleted=" + onCompleted);
        if (onCompleted != null) {
          onCompleted.run();
        }
      }
    }

    @Override
    public void _stop(final Task onCompleted) {
      trace("stopping.. at state: "+socketState);
      socketState.onStop(onCompleted);
    }

    protected String resolveHostName(String host) throws UnknownHostException {
        if (isUseLocalHost()) {
            String localName = getLocalHost().getHostName();
            if (localName != null && localName.equals(host)) {
                return "localhost";
            }
        }
        return host;
    }

    protected void onConnected() throws IOException {
        yieldSource = Dispatch.createSource(EventAggregators.INTEGER_ADD, dispatchQueue);
        String taskName = Task.DEBUG_TASK ? "yieldSource event handler for " + dispatchQueue.getLabel() : null;
        yieldSource.setEventHandler(new Task(taskName) {
            @Override
            public void run() {
                drainInbound();
            }
        });
        yieldSource.resume();
        drainOutboundSource = Dispatch.createSource(EventAggregators.INTEGER_ADD, dispatchQueue);
        taskName = Task.DEBUG_TASK ? "drainOutboundSource event handler for " + dispatchQueue.getLabel() : null;
        drainOutboundSource.setEventHandler(new Task(taskName) {
            @Override
            public void run() {
                flush();
            }
        });
        drainOutboundSource.resume();

        readSource = Dispatch.createSource(channel, SelectionKey.OP_READ, dispatchQueue);
        writeSource = Dispatch.createSource(channel, SelectionKey.OP_WRITE, dispatchQueue);

        readSource.setCancelHandler(CANCEL_HANDLER);
        writeSource.setCancelHandler(CANCEL_HANDLER);

        taskName = Task.DEBUG_TASK ? "readSource event handler for " + dispatchQueue.getLabel() : null;
        readSource.setEventHandler(new Task(taskName) {
            @Override
            public void run() {
                drainInbound();
            }
        });
        taskName = Task.DEBUG_TASK ? "writeSource event handler for " + dispatchQueue.getLabel() : null;
        writeSource.setEventHandler(new Task(taskName) {
            @Override
            public void run() {
                flush();
            }
        });

        initRateLimitingChannel();
        if( rateLimitingChannel!=null ) {
            schedualRateAllowanceReset();
        }
        listener.onTransportConnected();
    }

    private void schedualRateAllowanceReset() {
        String taskName = Task.DEBUG_TASK ? "reset schedule rate for " + dispatchQueue.getLabel() : null;
        dispatchQueue.executeAfter(1, TimeUnit.SECONDS, new Task(taskName){
            @Override
            public void run() {
                if( !socketState.is(CONNECTED.class) ) {
                    return;
                }
                rateLimitingChannel.resetAllowance();
                schedualRateAllowanceReset();
            }
        });
    }

    private void dispose() {
        if( readSource!=null ) {
            readSource.cancel();
            readSource=null;
        }

        if( writeSource!=null ) {
            writeSource.cancel();
            writeSource=null;
        }
    }

    public void onTransportFailure(Exception error) {
        // upcall the listener which should eventually call transport.stop
        listener.onTransportFailure(error instanceof IOException ? (IOException) error : new IOException(error));
    }


    @Override
    public boolean full() {
        return codec==null ||
               codec.full() ||
               !socketState.is(CONNECTED.class) ||
               getServiceState() != STARTED;
    }

    boolean rejectingOffers;

    @Override
    public boolean offer(Object command) {
        dispatchQueue.assertExecuting();
        if( full() ) {
            return false;
        }
        try {
            ProtocolCodec.BufferState rc = codec.write(command);
            rejectingOffers = codec.full();
            switch (rc ) {
                case FULL:
                    return false;
                default:
                    drainOutboundSource.merge(1);
            }
        } catch (IOException | RuntimeException e) {
            onTransportFailure(e);
        }
        return true;
    }

    boolean writeResumedForCodecFlush = false;

    /**
     *
     */
    @Override
    public void flush() {
        dispatchQueue.assertExecuting();
        if (getServiceState() != STARTED || !socketState.is(CONNECTED.class)) {
            return;
        }
        try {
            if( codec.flush() == ProtocolCodec.BufferState.EMPTY && transportFlush() ) {
                if( writeResumedForCodecFlush) {
                    writeResumedForCodecFlush = false;
                    suspendWrite();
                }
                rejectingOffers = false;
                listener.onRefill();

            } else {
                if(!writeResumedForCodecFlush) {
                    writeResumedForCodecFlush = true;
                    resumeWrite();
                }
            }
        } catch (IOException | RuntimeException e) {
            onTransportFailure(e);
        }
    }

    protected boolean transportFlush() throws IOException {
        return true;
    }

    @Override
    public void drainInbound() {
      trace("TcpT.drainInbound start");
      if (!getServiceState().isStarted() || readSource.isSuspended()) {
        return;
      }
      // drainInbound is called on OP_READ nio events, which includes end of stream and connection closed
      // we could try to report such terminal events in the suspended state, however most (all?) of the time
      // the nio interest is no longer registered in the suspended state
      try {
        long initial = codec.getReadCounter();
        // Only process up to 4 x the read buffer worth of data at a time so we can give
        // other connections a chance to process their requests.
        while( codec.getReadCounter()-initial < codec.getReadBufferSize()<<2 ) {
          Object command = codec.read();
          if ( command!=null ) {
            try {
              trace("TcpT.drainInbound onTransportCommand");
              listener.onTransportCommand(command);
            } catch (Throwable e) {
              e.printStackTrace();
              onTransportFailure(e instanceof Exception ? (Exception) e : new IOException("Transport listener failure."));
            }

            // the transport may be suspended after processing a command.
            if (getServiceState() == STOPPED || readSource.isSuspended()) {
              return;
            }
          } else {
            return;
          }
        }
        yieldSource.merge(1);
      } catch (IOException | RuntimeException e) {
        onTransportFailure(e);
      }
    }

    @Override
    public SocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    private boolean assertConnected() {
        try {
            if ( !isConnected() ) {
                throw new IOException("Not connected.");
            }
            return true;
        } catch (IOException e) {
            onTransportFailure(e);
        }
        return false;
    }

    @Override
    public void suspendRead() {
        if( isConnected() && readSource!=null ) {
            readSource.suspend();
        }
    }


    @Override
    public void resumeRead() {
        if( isConnected() && readSource!=null ) {
            if( rateLimitingChannel!=null ) {
                rateLimitingChannel.resumeRead();
            } else {
                _resumeRead();
            }
        }
    }

    private void _resumeRead() {
        readSource.resume();
        String taskName = Task.DEBUG_TASK ? "resume read for " + dispatchQueue.getLabel() : null;
        dispatchQueue.execute(new Task(taskName) {
            @Override
            public void run() {
                drainInbound();
            }
        });
    }

    protected void suspendWrite() {
        if( isConnected() && writeSource!=null ) {
            writeSource.suspend();
        }
    }

    protected void resumeWrite() {
        if( isConnected() && writeSource!=null ) {
            writeSource.resume();
        }
    }

    @Override
    public TransportListener getTransportListener() {
        return listener;
    }

    @Override
    public void setTransportListener(TransportListener transportListener) {
        this.listener = transportListener;
    }

    @Override
    public ProtocolCodec getProtocolCodec() {
        return codec;
    }

    @Override
    public void setProtocolCodec(ProtocolCodec protocolCodec) throws Exception {
        this.codec = protocolCodec;
        if( channel!=null && codec!=null ) {
            initializeCodec();
        }
    }

    @Override
    public boolean isConnected() {
        return socketState.is(CONNECTED.class);
    }

    @Override
    public boolean isClosed() {
        return getServiceState() == STOPPED;
    }

    public boolean isUseLocalHost() {
        return useLocalHost;
    }

    /**
     * Sets whether 'localhost' or the actual local host name should be used to
     * make local connections. On some operating systems such as Macs its not
     * possible to connect as the local host name so localhost is better.
     */
    public void setUseLocalHost(boolean useLocalHost) {
        this.useLocalHost = useLocalHost;
    }

    private static final Logger logger = Logger.getLogger("org.fusesource.hawtdispatch.transport");
    private static final boolean DEBUG  = logger.isLoggable(Level.FINE);
    private final void trace(String message) {
      if (DEBUG)
        logger.fine(format("%1$0#10x | %2$s", System.identityHashCode(this), message));
    }

    public SocketChannel getSocketChannel() {
        return channel;
    }

    @Override
    public ReadableByteChannel getReadChannel() {
        initRateLimitingChannel();
        if(rateLimitingChannel!=null) {
            return rateLimitingChannel;
        } else {
            return channel;
        }
    }

    @Override
    public WritableByteChannel getWriteChannel() {
        initRateLimitingChannel();
        if(rateLimitingChannel!=null) {
            return rateLimitingChannel;
        } else {
            return channel;
        }
    }

    public int getMaxReadRate() {
        return maxReadRate;
    }

    public void setMaxReadRate(int maxReadRate) {
        this.maxReadRate = maxReadRate;
    }

    public int getMaxWriteRate() {
        return maxWriteRate;
    }

    public void setMaxWriteRate(int maxWriteRate) {
        this.maxWriteRate = maxWriteRate;
    }

    public int getTrafficClass() {
        return trafficClass;
    }

    public void setTrafficClass(int trafficClass) {
        this.trafficClass = trafficClass;
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
                channel.socket().setSendBufferSize(sendBufferSize);
            } catch (SocketException ignore) {
            }
        }
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public Executor getBlockingExecutor() {
        return blockingExecutor;
    }

    @Override
    public void setBlockingExecutor(Executor blockingExecutor) {
        this.blockingExecutor = blockingExecutor;
    }

    public boolean isCloseOnCancel() {
        return closeOnCancel;
    }

    public void setCloseOnCancel(boolean closeOnCancel) {
        this.closeOnCancel = closeOnCancel;
    }
}
