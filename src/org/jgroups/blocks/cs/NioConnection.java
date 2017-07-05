package org.jgroups.blocks.cs;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Version;
import org.jgroups.nio.Buffers;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.CondVar;
import org.jgroups.util.Condition;
import org.jgroups.util.Util;

/**
 * An NIO based impl of {@link Connection}
 *
 * @author Bela Ban
 * @since 3.6.5
 */

// TODO: 17/5/25 by zmyer
public class NioConnection extends Connection {
    protected SocketChannel channel;      // the channel to the peer
    protected SelectionKey key;
    protected final NioBaseServer server;

    final Buffers send_buf;     // send messages via gathering writes
    private boolean write_interest_set; // set when a send() didn't manage to send all data
    private boolean copy_on_partial_write = true;
    private int partial_writes; // number of partial writes (write which did not write all bytes)
    private final Lock send_lock = new ReentrantLock(); // serialize send()

    // creates an array of 2: length buffer (for reading the length of the following data buffer) and data buffer
    // protected Buffers             recv_buf=new Buffers(2).add(ByteBuffer.allocate(Global.INT_SIZE), null);
    Buffers recv_buf = new Buffers(4).add(ByteBuffer.allocate(cookie.length));
    //数据读取对象
    protected Reader reader = new Reader(); // manages the thread which receives messages
    private long reader_idle_time = 20000; // number of ms a reader can be idle (no msgs) until it terminates

    /** Creates a connection stub and binds it, use {@link #connect(Address)} to connect */
    public NioConnection(Address peer_addr, NioBaseServer server) throws Exception {
        this.server = server;
        if (peer_addr == null)
            throw new IllegalArgumentException("Invalid parameter peer_addr=" + null);
        this.peer_addr = peer_addr;
        send_buf = new Buffers(server.maxSendBuffers() * 2); // space for actual bufs and length bufs!
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        setSocketParameters(channel.socket());
        last_access = getTimestamp(); // last time a message was sent or received (ns)
    }

    // TODO: 17/5/25 by zmyer
    public NioConnection(SocketChannel channel, NioBaseServer server) throws Exception {
        this.channel = channel;
        this.server = server;
        setSocketParameters(this.channel.socket());
        channel.configureBlocking(false);
        send_buf = new Buffers(server.maxSendBuffers() * 2); // space for actual bufs and length bufs!
        this.peer_addr = server.usePeerConnections() ? null /* read by first receive() */
            : new IpAddress((InetSocketAddress) channel.getRemoteAddress());
        last_access = getTimestamp(); // last time a message was sent or received (ns)
    }

    @Override
    public boolean isOpen() {
        return channel != null && channel.isOpen();
    }

    @Override
    public boolean isConnected() {
        return channel != null && channel.isConnected();
    }

    // TODO: 17/5/25 by zmyer
    @Override
    public boolean isExpired(long now) {
        return server.connExpireTime() > 0 && now - last_access >= server.connExpireTime();
    }

    // TODO: 17/5/25 by zmyer
    private void updateLastAccessed() {
        if (server.connExpireTime() > 0)
            last_access = getTimestamp();
    }

    @Override
    public Address localAddress() {
        InetSocketAddress local_addr = null;
        if (channel != null) {
            try {
                local_addr = (InetSocketAddress) channel.getLocalAddress();
            } catch (IOException ignored) {
            }
        }
        return local_addr != null ? new IpAddress(local_addr) : null;
    }

    public Address peerAddress() {
        return peer_addr;
    }

    public SelectionKey key() {
        return key;
    }

    // TODO: 17/5/25 by zmyer
    public NioConnection key(SelectionKey k) {
        this.key = k;
        return this;
    }

    NioConnection copyOnPartialWrite(boolean b) {
        this.copy_on_partial_write = b;
        return this;
    }

    public boolean copyOnPartialWrite() {
        return copy_on_partial_write;
    }

    int numPartialWrites() {
        return partial_writes;
    }

    public long readerIdleTime() {
        return reader_idle_time;
    }

    public NioConnection readerIdleTime(long t) {
        this.reader_idle_time = t;
        return this;
    }

    private boolean readerRunning() {
        return this.reader.isRunning();
    }

    // TODO: 17/5/25 by zmyer
    private synchronized void registerSelectionKey(int interest_ops) {
        if (key == null)
            return;
        key.interestOps(key.interestOps() | interest_ops);
    }

    // TODO: 17/5/25 by zmyer
    synchronized void clearSelectionKey(int interest_ops) {
        if (key == null)
            return;
        //从选择键值中清理指定的事件类型
        key.interestOps(key.interestOps() & ~interest_ops);
    }

    @Override
    public void connect(Address dest) throws Exception {
        connect(dest, server.usePeerConnections());
    }

    protected void connect(Address dest, boolean send_local_addr) throws Exception {
        SocketAddress destAddr = new InetSocketAddress(((IpAddress) dest).getIpAddress(), ((IpAddress) dest).getPort());
        try {
            if (!server.deferClientBinding())
                this.channel.bind(new InetSocketAddress(server.clientBindAddress(), server.clientBindPort()));
            if (this.channel.getLocalAddress() != null && this.channel.getLocalAddress().equals(destAddr))
                throw new IllegalStateException("socket's bind and connect address are the same: " + destAddr);

            this.key = server.register(channel, SelectionKey.OP_CONNECT | SelectionKey.OP_READ, this);
            if (Util.connect(channel, destAddr) && channel.finishConnect()) {
                clearSelectionKey(SelectionKey.OP_CONNECT);
            }
            if (send_local_addr)
                sendLocalAddress(server.localAddress());
        } catch (Exception t) {
            close();
            throw t;
        }
    }

    // TODO: 17/5/25 by zmyer
    @Override
    public void start() throws Exception {
        ; // nothing to be done here
    }

    // TODO: 17/5/25 by zmyer
    @Override
    public void send(byte[] buf, int offset, int length) throws Exception {
        send(ByteBuffer.wrap(buf, offset, length));
    }

    /**
     * Sends a message. If the previous write didn't complete, tries to complete it. If this still
     * doesn't complete, the message is dropped (needs to be retransmitted, e.g. by UNICAST3 or
     * NAKACK2).
     *
     * @param buf byte buffer
     * @throws Exception exception
     */
    // TODO: 17/5/25 by zmyer
    @Override
    public void send(ByteBuffer buf) throws Exception {
        send(buf, true);
    }

    // TODO: 17/5/25 by zmyer
    public void send() throws Exception {
        send_lock.lock();
        try {
            boolean success = send_buf.write(channel);
            writeInterest(!success);
            if (success)
                updateLastAccessed();
            if (!success) {
                if (copy_on_partial_write)
                    send_buf.copy(); // copy data on partial write as subsequent writes might corrupt data (https://issues.jboss.org/browse/JGRP-1991)
                partial_writes++;
            }
        } finally {
            send_lock.unlock();
        }
    }

    /**
     * Read the length first, then the actual data. This method is not reentrant and access must be
     * synchronized
     */
    // TODO: 17/5/25 by zmyer
    public void receive() throws Exception {
        reader.receive();
    }

    // TODO: 17/5/25 by zmyer
    protected void send(ByteBuffer buf, boolean send_length) throws Exception {
        send_lock.lock();
        try {
            // makeLengthBuffer() reuses the same pre-allocated buffer and copies it only if the write didn't complete
            if (send_length)
                send_buf.add(makeLengthBuffer(buf), buf);
            else
                send_buf.add(buf);
            boolean success = send_buf.write(channel);
            writeInterest(!success);
            if (success)
                updateLastAccessed();
            if (!success) {
                if (copy_on_partial_write)
                    send_buf.copy(); // copy data on partial write as subsequent writes might corrupt data (https://issues.jboss.org/browse/JGRP-1991)
                partial_writes++;
            }
        } finally {
            send_lock.unlock();
        }
    }

    // TODO: 17/5/25 by zmyer
    private boolean _receive(boolean update) throws Exception {
        ByteBuffer msg;
        //获取接收者
        Receiver receiver = server.receiver();

        if (peer_addr == null && server.usePeerConnections()
            && (peer_addr = readPeerAddress()) != null) {
            recv_buf = new Buffers(2).add(ByteBuffer.allocate(Global.INT_SIZE), null);
            //将对端的ip地址插入到连接列表中
            server.addConnection(peer_addr, this);
            return true;
        }

        //读取数据长度,并进行必要的扩容操作
        if ((msg = recv_buf.readLengthAndData(channel)) == null)
            return false;
        if (receiver != null)
            //开始读取数据
            receiver.receive(peer_addr, msg);
        if (update)
            //变更访问时间戳
            updateLastAccessed();
        return true;
    }

    @Override
    public void close() throws IOException {
        send_lock.lock();
        try {
            if (send_buf.remaining() > 0) { // try to flush send buffer if it still has pending data to send
                try {
                    send();
                } catch (Throwable ignored) {
                }
            }
            Util.close(channel, reader);
        } finally {
            send_lock.unlock();
        }
    }

    public String toString() {
        InetSocketAddress local = null, remote = null;
        try {
            local = channel != null ? (InetSocketAddress) channel.getLocalAddress() : null;
        } catch (Throwable ignored) {
        }
        try {
            remote = channel != null ? (InetSocketAddress) channel.getRemoteAddress() : null;
        } catch (Throwable ignored) {
        }
        String loc = local == null ? "n/a" : local.getHostString() + ":" + local.getPort(),
            rem = remote == null ? "n/a" : remote.getHostString() + ":" + remote.getPort();
        return String.format("<%s --> %s> (%d secs old) [%s] [recv_buf: %d, reader=%b]",
            loc, rem, TimeUnit.SECONDS.convert(getTimestamp() - last_access, TimeUnit.NANOSECONDS),
            status(), recv_buf.get(1) != null ? recv_buf.get(1).capacity() : 0, readerRunning());
    }

    protected String status() {
        if (channel == null)
            return "n/a";
        if (isConnected())
            return "connected";
        if (channel.isConnectionPending())
            return "connection pending";
        if (isOpen())
            return "open";
        return "closed";
    }

    // TODO: 17/5/25 by zmyer
    protected long getTimestamp() {
        return server.timeService() != null ?
            server.timeService().timestamp() : System.nanoTime();
    }

    // TODO: 17/5/25 by zmyer
    private void writeInterest(boolean register) {
        if (register) {
            if (!write_interest_set) {
                write_interest_set = true;
                registerSelectionKey(SelectionKey.OP_WRITE);
            }
        } else {
            if (write_interest_set) {
                write_interest_set = false;
                clearSelectionKey(SelectionKey.OP_WRITE);
            }
        }
    }

    private void setSocketParameters(Socket client_sock) throws SocketException {
        try {
            client_sock.setSendBufferSize(server.sendBufferSize());
        } catch (IllegalArgumentException ex) {
            server.log().error("%s: exception setting send buffer to %d bytes: %s", server.localAddress(), server.sendBufferSize(), ex);
        }
        try {
            client_sock.setReceiveBufferSize(server.receiveBufferSize());
        } catch (IllegalArgumentException ex) {
            server.log().error("%s: exception setting receive buffer to %d bytes: %s", server.localAddress(), server.receiveBufferSize(), ex);
        }

        client_sock.setKeepAlive(true);
        client_sock.setTcpNoDelay(server.tcpNodelay());
        if (server.linger() > 0)
            client_sock.setSoLinger(true, server.linger());
        else
            client_sock.setSoLinger(false, -1);
    }

    void sendLocalAddress(Address local_addr) throws Exception {
        try {
            int addr_size = local_addr.serializedSize();
            int expected_size = cookie.length + Global.SHORT_SIZE * 2 + addr_size;
            ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(expected_size + 2);
            out.write(cookie, 0, cookie.length);
            out.writeShort(Version.version);
            out.writeShort(addr_size); // address size
            local_addr.writeTo(out);
            ByteBuffer buf = out.getByteBuffer();
            send(buf, false);
            updateLastAccessed();
        } catch (Exception ex) {
            close();
            throw ex;
        }
    }

    // TODO: 17/5/25 by zmyer
    private Address readPeerAddress() throws Exception {
        while (recv_buf.read(channel)) {
            //开始读取通道内容
            int current_position = recv_buf.position() - 1;
            ByteBuffer buf = recv_buf.get(current_position);
            if (buf == null)
                return null;
            buf.flip();
            switch (current_position) {
                case 0:      // cookie
                    byte[] cookie_buf = getBuffer(buf);
                    if (!Arrays.equals(cookie, cookie_buf))
                        throw new IllegalStateException("BaseServer.NioConnection.readPeerAddress(): cookie read by "
                            + server.localAddress() + " does not match own cookie; terminating connection");
                    recv_buf.add(ByteBuffer.allocate(Global.SHORT_SIZE));
                    break;
                case 1:      // version
                    //读取版本号
                    short version = buf.getShort();
                    if (!Version.isBinaryCompatible(version))
                        throw new IOException("packet from " + channel.getRemoteAddress() + " has different version (" + Version.print(version) +
                            ") from ours (" + Version.printVersion() + "); discarding it");
                    recv_buf.add(ByteBuffer.allocate(Global.SHORT_SIZE));
                    break;
                case 2:      // length of address
                    //读取地址长度
                    short addr_len = buf.getShort();
                    recv_buf.add(ByteBuffer.allocate(addr_len));
                    break;
                case 3:      // address
                    //读取地址信息
                    byte[] addr_buf = getBuffer(buf);
                    ByteArrayDataInputStream in = new ByteArrayDataInputStream(addr_buf);
                    IpAddress addr = new IpAddress();
                    addr.readFrom(in);
                    return addr;
                default:
                    throw new IllegalStateException(String.format("position %d is invalid", recv_buf.position()));
            }
        }
        return null;
    }

    protected static byte[] getBuffer(final ByteBuffer buf) {
        byte[] retval = new byte[buf.limit()];
        buf.get(retval, buf.position(), buf.limit());
        return retval;
    }

    // TODO: 17/5/25 by zmyer
    private static ByteBuffer makeLengthBuffer(ByteBuffer buf) {
        return (ByteBuffer) ByteBuffer.allocate(Global.INT_SIZE).putInt(buf.remaining()).clear();
    }

    // TODO: 17/5/25 by zmyer
    protected enum State {
        reading, waiting_to_terminate, done
    }

    // TODO: 17/5/25 by zmyer
    private class Reader implements Runnable, Closeable {
        //重入锁对象
        protected final Lock lock = new ReentrantLock(); // to synchronize receive() and state transitions
        //状态对象
        protected State state = State.done;
        //数据是否可用
        volatile boolean data_available = true;
        //数据可用条件变量
        final CondVar data_available_cond = new CondVar();
        //执行线程
        protected volatile Thread thread;
        //是否运行
        protected volatile boolean running;

        // TODO: 17/5/25 by zmyer
        protected void start() {
            //设置启动标记
            running = true;
            //创建执行线程
            thread = server.factory.newThread(this,
                String.format("NioConnection.Reader [%s]", peer_addr));
            //设置守护模式
            thread.setDaemon(true);
            //启动线程
            thread.start();
        }

        // TODO: 17/5/25 by zmyer
        protected void stop() {
            //设置关闭标记
            running = false;
            //设置数据不可用
            data_available = true;
            //触发条件变量
            data_available_cond.signal(false);
        }

        // TODO: 17/5/25 by zmyer
        public void close() throws IOException {
            stop();
        }

        // TODO: 17/5/25 by zmyer
        public boolean isRunning() {
            Thread tmp = thread;
            return tmp != null && tmp.isAlive();
        }

        /** Called by the selector when data is ready to be read from the SocketChannel */
        // TODO: 17/5/25 by zmyer
        public void receive() {
            lock.lock();
            try {
                data_available = true;
                // only a single receive() at a time, until OP_READ is registered again (by the reader thread)
                //从选择键值中清理可读属性
                clear(SelectionKey.OP_READ);
                switch (state) {
                    case reading:
                        //如果当前的状态处在读取中,则直接跳过
                        break;
                    case waiting_to_terminate:
                        //如果是等待结束标记,则触发条件变量
                        data_available_cond.signal(false); // only 1 consumer
                        break;
                    case done:
                        // make sure the selector doesn't wake up for our connection while the reader is reading msgs
                        //如果是开始,则设置读取中标记
                        state = State.reading;
                        //开始读取数据
                        start();
                        break;
                }
            } finally {
                lock.unlock();
            }
        }

        // TODO: 17/5/25 by zmyer
        public void run() {
            try {
                //开始启动线程
                _run();
            } finally {
                register(SelectionKey.OP_READ);
            }
        }

        // TODO: 17/5/25 by zmyer
        void _run() {
            //数据是否准备就绪
            final Condition is_data_available = () -> data_available;
            while (running) {
                for (; ; ) { // try to receive as many msgs as possible, until no more msgs are ready or the conn is closed
                    try {
                        //开始接收数据
                        if (!_receive(false))
                            break;
                    } catch (Throwable ex) {
                        server.closeConnection(NioConnection.this, ex);
                        state(State.done);
                        return;
                    }
                }

                //更新最近一次访问时间戳
                updateLastAccessed();

                // Transition to state waiting_to_terminate and wait for server.readerIdleTime() ms
                //设置当前的状态为等待结束
                state(State.waiting_to_terminate);
                //数据不可用
                data_available = false;
                //重新注册可读属性
                register(SelectionKey.OP_READ); // now we might get receive() calls again
                //等待数据可读
                if (data_available_cond.waitFor(is_data_available, server.readerIdleTime(), TimeUnit.MILLISECONDS))
                    //设置读取中状态
                    state(State.reading);
                else {
                    //重新设置起始状态
                    state(State.done);
                    return;
                }
            }
        }

        // TODO: 17/5/25 by zmyer
        protected void register(int op) {
            try {
                registerSelectionKey(op);
                key.selector().wakeup(); // no-op if the selector is not blocked in select()
            } catch (Throwable ignored) {
            }
        }

        // TODO: 17/5/25 by zmyer
        protected void clear(int op) {
            try {
                clearSelectionKey(op);
            } catch (Throwable ignored) {
            }
        }

        // TODO: 17/5/25 by zmyer
        protected void state(State st) {
            lock.lock();
            try {
                this.state = st;
            } finally {
                lock.unlock();
            }
        }
    }
}
