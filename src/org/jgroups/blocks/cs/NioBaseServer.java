package org.jgroups.blocks.cs;

import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jgroups.Address;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;

/**
 * @author Bela Ban
 * @since 3.6.5
 */

// TODO: 17/5/25 by zmyer
public abstract class NioBaseServer extends BaseServer {
    //多路选择器对象
    protected Selector selector; // get notified about accepts, data ready to read/write etc
    //连接接收器对象
    protected Thread acceptor; // the thread which calls select() in a loop
    //重入锁
    private final Lock reg_lock = new ReentrantLock(); // for OP_CONNECT registrations
    //注册标记
    private volatile boolean registration; // set to true after a registration; the acceptor sets it back to false

    @ManagedAttribute(description = "Max number of send buffers. Changing this value affects new buffers only", writable = true)
    //最大的发送缓冲区数量
    private int max_send_buffers = 5; // size of WriteBuffers send buffer array

    @ManagedAttribute(description = "Number of times select() was called")
    //select调用次数
    private int num_selects;

    private boolean copy_on_partial_write = true;
    //读取空闲等待时间
    private long reader_idle_time = 20000;

    // TODO: 17/5/25 by zmyer
    NioBaseServer(ThreadFactory f, SocketFactory sf) {
        super(f, sf);
    }

    // TODO: 17/5/25 by zmyer
    int maxSendBuffers() {
        return max_send_buffers;
    }

    // TODO: 17/5/25 by zmyer
    public NioBaseServer maxSendBuffers(int num) {
        this.max_send_buffers = num;
        return this;
    }

    // TODO: 17/5/25 by zmyer
    public boolean selectorOpen() {
        return selector != null && selector.isOpen();
    }

    // TODO: 17/5/25 by zmyer
    public boolean acceptorRunning() {
        return acceptor != null && acceptor.isAlive();
    }

    // TODO: 17/5/25 by zmyer
    public int numSelects() {
        return num_selects;
    }

    // TODO: 17/5/25 by zmyer
    public boolean copyOnPartialWrite() {
        return copy_on_partial_write;
    }

    // TODO: 17/5/25 by zmyer
    long readerIdleTime() {
        return reader_idle_time;
    }

    // TODO: 17/5/25 by zmyer
    public NioBaseServer readerIdleTime(long t) {
        reader_idle_time = t;
        return this;
    }

    // TODO: 17/5/25 by zmyer
    public NioBaseServer copyOnPartialWrite(boolean b) {
        this.copy_on_partial_write = b;
        synchronized (this) {
            for (Connection c : conns.values()) {
                NioConnection conn = (NioConnection) c;
                conn.copyOnPartialWrite(b);
            }
        }
        return this;
    }

    // TODO: 17/5/25 by zmyer
    public synchronized int numPartialWrites() {
        int retval = 0;
        for (Connection c : conns.values()) {
            NioConnection conn = (NioConnection) c;
            retval += conn.numPartialWrites();
        }
        return retval;
    }

    /** Prints send and receive buffers for all connections */
    // TODO: 17/5/25 by zmyer
    @ManagedOperation(description = "Prints the send and receive buffers")
    public String printBuffers() {
        StringBuilder sb = new StringBuilder("\n");
        synchronized (this) {
            for (Map.Entry<Address, Connection> entry : conns.entrySet()) {
                NioConnection val = (NioConnection) entry.getValue();
                sb.append(entry.getKey()).append(":\n  ").append("recv_buf: ").append(val.recv_buf)
                    .append("\n  send_buf: ").append(val.send_buf).append("\n");
            }
        }
        return sb.toString();
    }

    // TODO: 17/5/25 by zmyer
    protected SelectionKey register(SelectableChannel ch, int interest_ops,
        NioConnection conn) throws Exception {
        reg_lock.lock();
        try {
            registration = true;
            selector.wakeup(); // needed because registration will block until selector.select() returns
            return ch.register(selector, interest_ops, conn);
        } finally {
            reg_lock.unlock();
        }
    }

    // TODO: 17/5/25 by zmyer
    @Override
    protected NioConnection createConnection(Address dest) throws Exception {
        return new NioConnection(dest, this).copyOnPartialWrite(copy_on_partial_write);
    }

    // TODO: 17/5/25 by zmyer
    protected void handleAccept(SelectionKey key) throws Exception {
    }

    // TODO: 17/5/25 by zmyer
    protected class Acceptor implements Runnable {

        // TODO: 17/5/25 by zmyer
        public void run() {
            Iterator<SelectionKey> it;
            while (running.get() && doSelect()) {
                try {
                    //获取准备就绪的选择键值
                    it = selector.selectedKeys().iterator();
                } catch (Throwable ex) {
                    continue;
                }

                while (it.hasNext()) {
                    //读取选择键对象
                    SelectionKey key = it.next();
                    //从选择键值对象中读取连接对象
                    NioConnection conn = (NioConnection) key.attachment();
                    //删除选择键值迭代器
                    it.remove();
                    try {
                        //如果键值不正确,则直接退回
                        if (!key.isValid())
                            continue;
                        //如果是可读的键值
                        if (key.isReadable())
                            //接收消息
                            conn.receive();
                        if (key.isWritable())
                            //发送消息
                            conn.send();
                        if (key.isAcceptable())
                            //接收链接
                            handleAccept(key);
                        else if (key.isConnectable()) {
                            //如果当前的键值为可连接状态,则读取对应的通道对象
                            SocketChannel ch = (SocketChannel) key.channel();
                            if (ch.finishConnect() || ch.isConnected())
                                //如果当前的通道对象已经完成了连接或者连接已经建立,则清理连接对象上的链接标记
                                conn.clearSelectionKey(SelectionKey.OP_CONNECT);
                        }
                    } catch (Throwable ex) {
                        //如果有异常,则直接关闭连接
                        closeConnection(conn, ex);
                    }
                }
            }
        }

        // TODO: 17/5/25 by zmyer
        boolean doSelect() {
            try {
                //执行多路选择
                int num = selector.select();
                //递增多路选择执行次数
                num_selects++;
                checkforPendingRegistrations();
                if (num == 0)
                    return true;
            } catch (ClosedSelectorException closed_ex) {
                log.trace("selector was closed; acceptor terminating");
                return false;
            } catch (Throwable t) {
                log.warn("acceptor failure", t);
            }
            return true;
        }

        // TODO: 17/5/25 by zmyer
        void checkforPendingRegistrations() {
            if (registration) {
                reg_lock.lock(); // mostly uncontended -> fast
                try {
                    registration = false;
                } finally {
                    reg_lock.unlock();
                }
            }
        }
    }
}
