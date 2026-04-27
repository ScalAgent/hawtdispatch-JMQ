/**
 * Copyright (C) 2012 FuseSource, Inc.
 * http://fusesource.com
 * Copyright (C) 2026 ScalAgent Distributed Technologies
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

import java.io.EOFException;
import java.io.IOException;
import java.net.ProtocolException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.hawtdispatch.util.BufferPool;
import org.fusesource.hawtdispatch.util.BufferPools;

/**
 * Provides an abstract base class to make implementing the ProtocolCodec interface
 * easier.
 *
 * The interaction between this base class and the derived Codec has been clarified from the original
 * algorithm of the hawtdispatch project.
 *
 * The new API, standard mode, is defined as follows:
 * - the derived Codec designs its decoding algorithm as Action objects
 * - each Action apply function is upcalled by the base Codec with readBuffer as input
 * - the base Codec fills in readBuffer by reading from the socket before upcalling nextDecodeAction,
 *   updating the buffer position
 * - the active part of the buffer is defined from the index readStart to the buffer position
 * - the derived Codec may define the expected size of this active part for the proper execution of the next Action apply;
 *   it is provided by the bytesWanted instance variable, which is interpreted as a simple hint by the base codec,
 *   the apply function will be upcalled even if less bytes may be actually read from the socket
 * - the base codec may resize and reallocate readBuffer; during this process it may discard all data preceding readStart;
 *   the active part is guaranteed to be preserved, and readStart and readEnd are updated accordingly for the new buffer
 * - the ultimate objective of an Action is to build a command; when it actually does so, it updates readStart
 *   to point to the beginning of the next message
 * - an Action may also represent a simple step in the process of building the command; if it succeeds in this step,
 *   it may update the readStart pointer, thus informing the base codec that part of the buffer could be discarded
 * - an Action may update the nextDecodeAction to set the next step of the decoding algorithm
 * - the first Action to upcall is provided by the abstract function initialDecodeAction
 *
 * The API provides two main modes relative to memory management, controlled by the forceCopy variable.
 * In the forceCopy=true mode, the base codec ensures that the active part of the buffer will remain available
 * and unchanged after the apply upcall returns; the buffer will eventually be freed by the Java garbage collector.
 * In the forceCopy=false mode, the status of the buffer is indicated by the readBufferReusable variable;
 * if it is true, the base codec may reuse the buffer as soon as the next call to the read function,
 * the derived codec must then make sure to copy the data that it wants to keep beyond this point;
 * if it is false, the derived codec may use the buffer as in the forceCopy=true mode.
 * The codec.read is expected to be called in the transport drainInbound loop, which immediately executes
 * the onTransportCommand upcall after reading a command. A reusable buffer may then be used by the derived codec
 * until the onTransportCommand upcall returns.
 *
 * The base codec read algorithm may be further configured by some variables which can be initially set in derived classes.
 * - bufferPools: activates a pool management of write and read buffers
 * - minReadSize: number of free bytes required before reallocating, if bytesWanted not relevant
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class AbstractProtocolCodec implements ProtocolCodec {

    // debug logging variables
    protected static final Logger readLogger = Logger.getLogger("org.fusesource.hawtdispatch.transport.AbstractProtocolCodec.read");
    protected static final boolean LOG_BUFFER_READ = readLogger.isLoggable(Level.FINEST);
    protected static final Logger copyLogger = Logger.getLogger("org.fusesource.hawtdispatch.transport.AbstractProtocolCodec.copy");
    protected static final boolean LOG_BUFFER_COPY = copyLogger.isLoggable(Level.FINEST);
    protected int commandNumber = 0;
    protected int readNumber = 0;
    protected int resizeNumber = 0;
    protected long resizeTotal = 0;

    protected BufferPools bufferPools;
    protected BufferPool writeBufferPool;
    protected BufferPool readBufferPool;

    protected int writeBufferSize = 1024 * 64;
    protected long writeCounter = 0L;
    protected GatheringByteChannel writeChannel = null;
    // current buffer to encode objects into
    protected DataByteArrayOutputStream nextWriteBuffer;
    protected long lastWriteIoSize = 0;

    // list of buffers to flush onto the output channel
    protected LinkedList<ByteBuffer> writeBuffer = new LinkedList<ByteBuffer>();
    private long writeBufferRemaining = 0;


    /**
     * Upcall interface to the derived Codec.
     */
    public static interface Action {
        /** apply the derived Codec action on the readBuffer */
        Object apply() throws IOException;
    }

    protected long readCounter = 0L;
    protected int readBufferSize = 1024 * 64;
    protected ReadableByteChannel readChannel = null;
    protected ByteBuffer readBuffer;
    protected ByteBuffer directReadBuffer = null;
    // if true, the current readBuffer is intended to go back into the readBufferPool
    // the derived Codec cannot keep reference to the readBuffer data after building a command
    // set by the base class, read by the derived classes
    protected boolean readBufferReusable = false;
    // minimum available size in the buffer before reading from the channel, reallocate if below
    // unused when bytesWanted is properly set, can be initially set in derived classes
    // the default value of 1 corresponds to the original algorithm, where an allocated buffer is completely
    // filled in before a new and bigger buffer is allocated.
    // A value of 25% of the default buffer size could be more efficient.
    protected int minReadSize = 1;
    // if true, the base Codec guarantees that the readBuffer is duplicated before the upcall to the derived Codec
    // the true value corresponds to the original algorithm
    // can be initially set in derived classes
    protected boolean forceCopy = true;

    // unclear semantics in the original read algorithm, we believe its goal was twofold:
    // - indicate the first byte in readBuffer unused by the derived class (main usage)
    // - indicate the expected end of the next message, possibly beyond the end of readBuffer (used in read)
    // it has been replaced in the new read algorithm by two elements:
    // - the lastApplied local variable in the read function
    // - the bytesWanted instance
    // the variable is kept as its main semantics because some functions of this class need it,
    // but it is no longer used in the algorithm of the read function
    protected int readEnd;

    // index of the first byte in readBuffer which is used as input by nextDecodeAction
    // bytes preceding readStart in readBuffer may be discarded by the base Codec when reallocating readBuffer.
    protected int readStart;
    // number of bytes read in the last channel.read call
    protected int lastReadIoSize;
    // next derived Codec Action to apply on readBuffer
    protected Action nextDecodeAction;
    // number of available bytes requested by the derived Codec to apply the Action,
    // may be 0, in which case the base Codec chooses a default size to read
    // initialized with 0 for backwards compatibility with the original algorithm
    protected int bytesWanted = 0;

    @Override
    public void setTransport(Transport transport) {
        this.writeChannel = (GatheringByteChannel) transport.getWriteChannel();
        this.readChannel = transport.getReadChannel();
        if( nextDecodeAction==null ) {
            nextDecodeAction = initialDecodeAction();
        }
        if( transport instanceof TcpTransport) {
            TcpTransport tcp = (TcpTransport) transport;
            writeBufferSize = tcp.getSendBufferSize();
            readBufferSize = tcp.getReceiveBufferSize();
        } else if( transport instanceof UdpTransport) {
            UdpTransport tcp = (UdpTransport) transport;
            writeBufferSize = tcp.getSendBufferSize();
            readBufferSize = tcp.getReceiveBufferSize();
        } else {
            try {
                if (this.writeChannel instanceof SocketChannel) {
                    writeBufferSize = ((SocketChannel) this.writeChannel).socket().getSendBufferSize();
                    readBufferSize = ((SocketChannel) this.readChannel).socket().getReceiveBufferSize();
                } else if (this.writeChannel instanceof SslTransport.SSLChannel) {
                    writeBufferSize = ((SslTransport.SSLChannel) this.readChannel).socket().getSendBufferSize();
                    readBufferSize = ((SslTransport.SSLChannel) this.writeChannel).socket().getReceiveBufferSize();
                }
            } catch (SocketException ignore) {
            }
        }
        if( bufferPools!=null ) {
            readBufferPool = bufferPools.getBufferPool(readBufferSize);
            writeBufferPool = bufferPools.getBufferPool(writeBufferSize);
        }
    }

    @Override
    public int getReadBufferSize() {
        return readBufferSize;
    }

    @Override
    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    @Override
    public boolean full() {
        return writeBufferRemaining >= writeBufferSize;
    }

    public boolean isEmpty() {
        return writeBufferRemaining == 0 && (nextWriteBuffer==null || nextWriteBuffer.size() == 0);
    }

    @Override
    public long getWriteCounter() {
        return writeCounter;
    }

    @Override
    public long getLastWriteSize() {
        return lastWriteIoSize;
    }

    abstract protected void encode(Object value) throws IOException;

    @Override
    public ProtocolCodec.BufferState write(Object value) throws IOException {
        if (full()) {
            return ProtocolCodec.BufferState.FULL;
        } else {
            boolean wasEmpty = isEmpty();
            if( nextWriteBuffer == null ) {
                nextWriteBuffer = allocateNextWriteBuffer();
            }
            encode(value);
            if (nextWriteBuffer.size() >= (writeBufferSize* 0.75)) {
                flushNextWriteBuffer();
            }
            if (wasEmpty) {
                return ProtocolCodec.BufferState.WAS_EMPTY;
            } else {
                return ProtocolCodec.BufferState.NOT_EMPTY;
            }
        }
    }

    private DataByteArrayOutputStream allocateNextWriteBuffer() {
        if( writeBufferPool !=null ) {
            return new DataByteArrayOutputStream(writeBufferPool.checkout()) {
                @Override
                protected void resize(int newcount) {
                    byte[] oldbuf = buf;
                    super.resize(newcount);
                    if( oldbuf.length == writeBufferPool.getBufferSize() ) {
                        writeBufferPool.checkin(oldbuf);
                    }
                }
            };
        } else {
            return new DataByteArrayOutputStream(writeBufferSize);
        }
    }

    protected void writeDirect(ByteBuffer value) throws IOException {
        // is the direct buffer small enough to just fit into the nextWriteBuffer?
        int nextnextPospos = nextWriteBuffer.position();
        int valuevalueLengthlength = value.remaining();
        int available = nextWriteBuffer.getData().length - nextnextPospos;
        if (available > valuevalueLengthlength) {
            value.get(nextWriteBuffer.getData(), nextnextPospos, valuevalueLengthlength);
            nextWriteBuffer.position(nextnextPospos + valuevalueLengthlength);
        } else {
            if (nextWriteBuffer!=null && nextWriteBuffer.size() != 0) {
                flushNextWriteBuffer();
            }
            writeBuffer.add(value);
            writeBufferRemaining += value.remaining();
        }
    }

    protected void flushNextWriteBuffer() {
        DataByteArrayOutputStream next = allocateNextWriteBuffer();
        ByteBuffer bb = nextWriteBuffer.toBuffer().toByteBuffer();
        writeBuffer.add(bb);
        writeBufferRemaining += bb.remaining();
        nextWriteBuffer = next;
    }

    @Override
    public ProtocolCodec.BufferState flush() throws IOException {
        while (true) {
            if (writeBufferRemaining != 0) {
                if( writeBuffer.size() == 1) {
                    ByteBuffer b = writeBuffer.getFirst();
                    lastWriteIoSize = writeChannel.write(b);
                    if (lastWriteIoSize == 0) {
                        return ProtocolCodec.BufferState.NOT_EMPTY;
                    } else {
                        writeBufferRemaining -= lastWriteIoSize;
                        writeCounter += lastWriteIoSize;
                        if(!b.hasRemaining()) {
                            onBufferFlushed(writeBuffer.removeFirst());
                        }
                    }
                } else {
                    ByteBuffer[] buffers = writeBuffer.toArray(new ByteBuffer[writeBuffer.size()]);
                    lastWriteIoSize = writeChannel.write(buffers, 0, buffers.length);
                    if (lastWriteIoSize == 0) {
                        return ProtocolCodec.BufferState.NOT_EMPTY;
                    } else {
                        writeBufferRemaining -= lastWriteIoSize;
                        writeCounter += lastWriteIoSize;
                        while (!writeBuffer.isEmpty() && !writeBuffer.getFirst().hasRemaining()) {
                            onBufferFlushed(writeBuffer.removeFirst());
                        }
                    }
                }
            } else {
                if (nextWriteBuffer==null || nextWriteBuffer.size() == 0) {
                    if( writeBufferPool!=null &&  nextWriteBuffer!=null ) {
                        writeBufferPool.checkin(nextWriteBuffer.getData());
                        nextWriteBuffer = null;
                    }
                    return ProtocolCodec.BufferState.EMPTY;
                } else {
                    flushNextWriteBuffer();
                }
            }
        }
    }

    /**
     * Called when a buffer is flushed out.  Subclasses can implement
     * in case they want to recycle the buffer.
     *
     * @param byteBuffer
     */
    protected void onBufferFlushed(ByteBuffer byteBuffer) {
    }

    /////////////////////////////////////////////////////////////////////
    //
    // Non blocking read impl
    //
    /////////////////////////////////////////////////////////////////////

    abstract protected Action initialDecodeAction();


    @Override
    public void unread(byte[] buffer) {
        assert ((readCounter == 0));
        readBuffer = ByteBuffer.allocate(buffer.length);
        readBuffer.put(buffer);
        readCounter += buffer.length;
    }

    @Override
    public long getReadCounter() {
        return readCounter;
    }

    @Override
    public long getLastReadSize() {
        return lastReadIoSize;
    }

    /**
     * Reads the next command from readChannel.
     * This function may return null if no input is available, or if not enough input is available
     * for decoding a command. The derived Codec is responsible for building the command through
     * the provided Action nextDecodeAction.
     * Building a command may require several upcalls to several Action objects, implementing the derived
     * Codec algorithm.
     */
    @Override
    public Object read() throws IOException {
      if (LOG_BUFFER_READ)
        logBufferRead("read start", ", bytesWanted=", Integer.toString(bytesWanted));
      // check first if the current readBuffer may be released
      if (readBufferReusable && readStart == readBuffer.position() && readStart > 0) {
        // this optimizes the case where a new message arrived in between successive calls to this read function
        // from the transport drainInbound loop
        // just reset the current buffer
        readBuffer.clear();
        readEnd = readStart = 0;
        if (LOG_BUFFER_READ)
          logBufferRead("read initial compact");
      }
      Object command = null;
      // keep track of the last byte processed by the derived Codec
      int lastApplied = readStart;
      while (command == null) {
        if (directReadBuffer != null) {
          // this part of the algorithm has been kept, but it is currently not used in the JMQ context
          // its proper use API is not clear, but it seems to be used when the derived Codec knows exactly
          // the number of bytes to read to build the command. This is the case for the JMQ derived Codec,
          // so maybe this option could be used
          // before using this mode, the interaction with the standard mode should be validated
          while (directReadBuffer.hasRemaining()) {
            lastReadIoSize = readChannel.read(directReadBuffer);
            if (lastReadIoSize == -1) {
              throw new EOFException("Peer disconnected");
            } else if (lastReadIoSize == 0) {
              return null;
            }
            // lastReadIoSize > 0
            readCounter += lastReadIoSize;
          }
          command = nextDecodeAction.apply();
        } else {
          if (LOG_BUFFER_READ)
            logBufferRead("read loop");
          // check if enough bytes are available for calling nextDecodeAction
          int loadedSize = readBuffer == null ? 0 : readBuffer.position() - readStart;
          int remainingSize = readBuffer == null ? 0 : readBuffer.remaining();
          int appliedSize = lastApplied - readStart;
          // no need to update lastApplied after this point

          if (loadedSize > appliedSize) {
            // more bytes are available than those which were used/available in the previous apply call
            // try to call apply without reading from the channel, whatever the value of the Action bytesWanted
            // we do not want to exit on a channel.read returning 0 if bytesWanted is not exactly set
          } else {
            // appliedSize=loadedSize, the last apply call returned null and did not move readStart
            // need to fetch more input
            int wantedSize = bytesWanted;
            boolean neededSizeUnknown = false;
            if (wantedSize <= loadedSize) {
              // the derived Codec may not know how much bytes are required, and it may choose to return 0.
              // The case is similar when the returned value is lower than what is actually needed.
              neededSizeUnknown = true;
              // In that case, we could try to apply nextDecodeAction with the available bytes but there is
              // a risk of infinite loop if the available bytes are not enough for the derived Codec.
              // this risk is prevented by the (loadedSize > appliedSize) test above.
              // At this point we do not know how much more bytes are necessary to build the command
              // we must estimate a proper amount of bytes to read
              if (readBuffer == null) {
                // request the default buffer size
                wantedSize = readBufferSize;
              } else {
                // we may choose to complete the current buffer or allocate a bigger one
                // we may also optimize the current buffer by cleaning data before readStart
                // allow for the configuration of the algorithm with the minReadSize variable.
                wantedSize = loadedSize + minReadSize;
              }
            }
            if (LOG_BUFFER_READ)
              logBufferRead("read loop",
                            ", loadedSize=", Integer.toString(loadedSize),
                            ", remainingSize=", Integer.toString(remainingSize),
                            ", appliedSize=", Integer.toString(appliedSize),
                            ", wantedSize=", Integer.toString(wantedSize));

            // wantedSize > loadedSize
            // check if the current buffer may fit
            if ((wantedSize - loadedSize) <= remainingSize) {
              // we can use the buffer
            } else if (readBufferReusable && (wantedSize - loadedSize) <= (remainingSize + readStart)) {
              // we can make enough room by compacting the the buffer
              if (loadedSize == 0) {
                readBuffer.clear();
                readEnd = readStart = 0;
              } else {
                if (LOG_BUFFER_COPY)
                  logBufferCopy("read loop compact", loadedSize, readBuffer.limit());
                readBuffer.flip().position(readStart);
                readBuffer.compact();
                readEnd -= readStart;
                readStart = 0;
              }
              if (LOG_BUFFER_READ)
                logBufferRead("read loop compact");
            } else {
              // need to reallocate a buffer
              int newSize;
              if (neededSizeUnknown) {
                // require a readBufferSize increment, to optimize the number of bytes read in a read operation
                newSize = loadedSize + readBufferSize;
              } else {
                // allocate the buffer at the bytesWanted size returned by the derived Codec
                newSize = Math.max(readBufferSize, wantedSize);
              }
              if (LOG_BUFFER_COPY && loadedSize > 0)
                logBufferCopy("read loop resize", loadedSize, newSize);
              byte[] newByteArray;
              ByteBuffer newBuffer;
              boolean newReadBufferReusable = false;
              if(readBufferPool != null && newSize == readBufferPool.getBufferSize()) {
                newByteArray = readBufferPool.checkout();
                newBuffer = ByteBuffer.wrap(newByteArray);
                newReadBufferReusable = true;
                if (loadedSize > 0) {
                  // copy the bytes already read, do not initialize the remaining bytes
                  // in the legacy algorithm, the remaining bytes are set to 0
                  newBuffer.put(readBuffer.array(), readStart, loadedSize);
                }
              } else {
                if (loadedSize > 0) {
                  // this call copies the bytes already read and the end of the readBuffer
                  // and initializes the additional bytes to 0
                  // I am not sure it is possible or desirable to limit the copy to only the read bytes
                  newByteArray = Arrays.copyOfRange(readBuffer.array(), readStart, readStart + newSize);
                } else {
                  newByteArray = new byte[newSize];
                }
                newBuffer = ByteBuffer.wrap(newByteArray);
                if (loadedSize > 0)
                  newBuffer.position(loadedSize);
              }
              if(readBufferReusable) {
                readBufferPool.checkin(readBuffer.array());
              }
              readBuffer = newBuffer;
              readBufferReusable = newReadBufferReusable;
              readEnd -= readStart;
              readStart = 0;
              if (LOG_BUFFER_READ)
                logBufferRead("read loop resize");
            }

            // actually read data from the socket
            if (LOG_BUFFER_COPY)
              readNumber++;
            lastReadIoSize = readChannel.read(readBuffer);
            if (LOG_BUFFER_READ)
              logBufferRead("read loop read");

            if (lastReadIoSize == -1) {
              // the connection has failed, the Codec will be released
              // free what must be explicitely freed
              if (readBufferReusable) {
                readBufferPool.checkin(readBuffer.array());
                readBuffer = null;
                if (LOG_BUFFER_READ)
                  logBufferRead("read loop EOF");
              }
              throw new EOFException("Peer disconnected");
            } else if (lastReadIoSize == 0) {
              if (readStart == readBuffer.position() && readStart > 0) {
                // no input has been read for the next command, good time to clean the current readBuffer
                if (readBufferReusable) {
                  // return the buffer to the pool
                  // keep in mind that the pool is local to the thread and may be used next by another Codec
                  readBufferPool.checkin(readBuffer.array());
                  readBufferReusable = false;
                }
                readEnd = readStart = 0;
                readBuffer = null;
                if (LOG_BUFFER_READ)
                  logBufferRead("read loop release");
              }
              return null;
            }
            // lastReadIoSize > 0
            readCounter += lastReadIoSize;

            if (forceCopy && readBufferReusable) {
              // the base Codec is responsible for duplicating the data before upcalling nextDecodeAction
              // duplicate exactly the read data to make sure that no additional data will be read after
              if (LOG_BUFFER_COPY) {
                int finalSize = readBuffer.position() - readStart;
                logBufferCopy("read loop downsize", finalSize, finalSize);
              }
              byte[] newByteArray = Arrays.copyOfRange(readBuffer.array(), readStart, readBuffer.position());
              ByteBuffer newBuffer = ByteBuffer.wrap(newByteArray);
              newBuffer.position(newByteArray.length);
              readBufferPool.checkin(readBuffer.array());
              readBuffer = newBuffer;
              readEnd -= readStart;
              readStart = 0;
              readBufferReusable = false;
              if (LOG_BUFFER_READ)
                logBufferRead("read loop final copy");
            }
          }

          // call the derived Codec
          int oldReadStart = readStart;
          lastApplied = readBuffer.position();
          command = nextDecodeAction.apply();
          if (command == null) {
            if (readStart > oldReadStart) {
              lastApplied = readStart;
              if (readBufferReusable && readStart == readBuffer.position()) {
                // the derived codec discarded some data, we may want to optimize the next read by resetting the buffer
                // just reset the current buffer
                readBuffer.clear();
                lastApplied = readStart = 0;
              }
            }
          }
          if (LOG_BUFFER_READ)
            logBufferRead("read loop applied");

          // if the read bytes are not enough for building a command, the derived codec returns null
          // in that case the base code immediately performs a new iteration
          // here is a case, observed during a test, which could be optimized:
          // a client send a 1MB message; the broker is available and immediately tries to read the message;
          // complete reading the message requires 16 loop iterations and separate channel.read calls,
          // a few of them returning 0 bytes. Is it possible to identify such a case early, to exit from
          // the reading loop, so that the next read returns more bytes and the total number of reads is reduced?
        }
      }
      if (LOG_BUFFER_COPY) {
        commandNumber++;
      }
      return command;
    }

    protected final void logBufferCopy(String reason, int copySize, int finalSize) {
      resizeNumber++;
      resizeTotal += copySize;
      copyLogger.finest("codec @" + Integer.toHexString(hashCode()) + " " +
                        reason + " for command #" + (commandNumber+1) +
                        " " + bufferSummary(readBuffer) +
                        ": " + copySize + " -> " + finalSize +
                        ", resize #" + resizeNumber + ", total size " + resizeTotal);
    }

    protected final void logBufferRead(String reason, String... msg) {
      StringBuilder builder = new StringBuilder();
      builder.append("codec @").append(Integer.toHexString(hashCode()));
      builder.append(reason);
      builder.append(", readBuffer=").append(bufferSummary(readBuffer));
      builder.append(" readStart=").append(readStart);
      builder.append(" readEnd=").append(readEnd);
      for (String s: msg)
        builder.append(s);
      readLogger.finest(builder.toString());
    }

    protected Buffer readUntil(Byte octet) throws ProtocolException {
        return readUntil(octet, -1);
    }

    protected Buffer readUntil(Byte octet, int max) throws ProtocolException {
        return readUntil(octet, max, "Maximum protocol buffer length exeeded");
    }

    protected Buffer readUntil(Byte octet, int max, String msg) throws ProtocolException {
        byte[] array = readBuffer.array();
        Buffer buf = new Buffer(array, readEnd, readBuffer.position() - readEnd);
        int pos = buf.indexOf(octet);
        if (pos >= 0) {
            int offset = readStart;
            readEnd += pos + 1;
            readStart = readEnd;
            int length = readEnd - offset;
            if (max >= 0 && length > max) {
                throw new ProtocolException(msg);
            }
            return new Buffer(array, offset, length);
        } else {
            readEnd += buf.length;
            if (max >= 0 && (readEnd - readStart) > max) {
                throw new ProtocolException(msg);
            }
            return null;
        }
    }

    protected Buffer readBytes(int length) {
        readEnd = readStart + length;
        if (readBuffer.position() < readEnd) {
            return null;
        } else {
            int offset = readStart;
            readStart = readEnd;
            return new Buffer(readBuffer.array(), offset, length);
        }
    }

    protected Buffer peekBytes(int length) {
        readEnd = readStart + length;
        if (readBuffer.position() < readEnd) {
            return null;
        } else {
            // rewind..
            readEnd = readStart;
            return new Buffer(readBuffer.array(), readStart, length);
        }
    }

    protected Boolean readDirect(ByteBuffer buffer) {
        assert (directReadBuffer == null || (directReadBuffer == buffer));

        if (buffer.hasRemaining()) {
            // First we need to transfer the read bytes from the non-direct
            // byte buffer into the direct one..
            int limit = readBuffer.position();
            int transferSize = Math.min((limit - readStart), buffer.remaining());
            byte[] readBufferArray = readBuffer.array();
            buffer.put(readBufferArray, readStart, transferSize);

            // The direct byte buffer might have been smaller than our readBuffer one..
            // compact the readBuffer to avoid doing additional mem allocations.
            int trailingSize = limit - (readStart + transferSize);
            if (trailingSize > 0) {
                System.arraycopy(readBufferArray, readStart + transferSize, readBufferArray, readStart, trailingSize);
            }
            ((java.nio.Buffer) readBuffer).position(readStart + trailingSize);
        }

        // For big direct byte buffers, it will still not have been filled,
        // so install it so that we directly read into it until it is filled.
        if (buffer.hasRemaining()) {
            directReadBuffer = buffer;
            return false;
        } else {
            directReadBuffer = null;
            ((java.nio.Buffer) buffer).flip();
            return true;
        }
    }

    public BufferPools getBufferPools() {
        return bufferPools;
    }

    public void setBufferPools(BufferPools bufferPools) {
        this.bufferPools = bufferPools;
        if( bufferPools!=null ) {
            readBufferPool = bufferPools.getBufferPool(readBufferSize);
            writeBufferPool = bufferPools.getBufferPool(writeBufferSize);
        } else {
            readBufferPool = null;
            writeBufferPool = null;
        }
    }

    // logging function
    protected String bufferSummary(ByteBuffer buf) {
      if (buf == null)
        return null;
      StringBuilder builder = new StringBuilder();
      builder.append('[').append(buf.array());
      builder.append(", pos=").append(buf.position());
      builder.append(", lim=").append(buf.limit());
      builder.append(", cap=").append(buf.capacity());
      builder.append(']');
      return builder.toString();
    }
}
