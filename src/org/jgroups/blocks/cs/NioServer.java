package org.jgroups.blocks.cs;

import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import org.jgroups.Address;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

/**
 * Server for sending and receiving messages via NIO channels. Uses only a single thread to accept,
 * connect, write and read connections. Read messages are passed to a receiver, which typically uses
 * a thread pool to process messages.<p/> Note that writes can get dropped, e.g. in the case where
 * we have a previous write pending and a new write is received. This is typically not an issue as
 * JGroups retransmits messages, but might become one when using NioServer standalone, ie. outside
 * of JGroups.
 *
 * @author Bela Ban
 * @since 3.6.5
 */
// TODO: 17/5/25 by zmyer
public class NioServer extends NioBaseServer {
    //服务器通道对象
    protected ServerSocketChannel channel;  // used to accept connections from peers

    /**
     * Creates an instance of NioServer.
     *
     * @param bind_addr The local bind address and port. If null, a bind address and port will be
     * picked by the OS.
     */
    // TODO: 17/5/25 by zmyer
    public NioServer(IpAddress bind_addr) throws Exception {
        this(bind_addr != null ? bind_addr.getIpAddress() : null,
            bind_addr != null ? bind_addr.getPort() : 0);
    }

    /**
     * Creates an instance of {@link NioServer} that opens a server channel and listens for
     * connections. Needs to be started next.
     *
     * @param bind_addr The local address to bind to. If null, the address will be picked by the OS
     * @param port The local port to bind to
     * @throws Exception Thrown if the creation failed
     */
    // TODO: 17/5/25 by zmyer
    public NioServer(InetAddress bind_addr, int port) throws Exception {
        this(new DefaultThreadFactory("nio", false), new DefaultSocketFactory(),
            bind_addr, port, port + 50, null, 0);
    }

    /**
     * Creates an instance of {@link NioServer} that opens a server channel and listens for
     * connections. Needs to be started next.
     *
     * @param thread_factory The thread factory used to create new threads
     * @param socket_factory The socket factory used to create socket channels
     * @param bind_addr The local address to bind to. If null, the address will be picked by the OS
     * @param srv_port The local port to bind to If 0, the port will be picked by the OS.
     * @param end_port If srv_port is taken, the next port is tried, until end_port has been
     * reached, in which case an exception will be thrown. If srv_port == end_port, only 1 port will
     * be tried.
     * @param external_addr The external address in case of NAT. Ignored if null.
     * @param external_port The external port on the NA. If 0, srv_port is used.
     * @throws Exception Thrown if the creation failed
     */
    // TODO: 17/5/25 by zmyer
    public NioServer(ThreadFactory thread_factory, SocketFactory socket_factory,
        InetAddress bind_addr, int srv_port, int end_port,
        InetAddress external_addr, int external_port) throws Exception {
        super(thread_factory, socket_factory);
        // channel=this.socket_factory.createServerSocketChannel("jgroups.nio.server");
        // channel.setOption(StandardSocketOptions.SO_REUSEADDR, reuse_addr);
        // Util.bind(channel, bind_addr, srv_port, end_port);
        //创建通道对象
        channel = Util.createServerSocketChannel(this.socket_factory,
            "jgroups.nio.server", bind_addr, srv_port, end_port);
        //设置通道非阻塞标记
        channel.configureBlocking(false);
        //创建多路选择器
        selector = Selector.open();
        //创建连接接收器对象
        acceptor = factory.newThread(new Acceptor(),
            "NioServer.Selector [" + channel.getLocalAddress() + "]");
        //将通道对象注册到多路选择器中
        channel.register(selector, SelectionKey.OP_ACCEPT, null);
        //获取本地地址
        local_addr = localAddress(bind_addr,
            channel.socket().getLocalPort(), external_addr, external_port);
    }

    protected NioServer(ThreadFactory f, SocketFactory sf) {
        super(f, sf);
    }

    // TODO: 17/5/25 by zmyer
    @Override
    protected void handleAccept(SelectionKey key) throws Exception {
        //接收对端连接
        SocketChannel client_channel = channel.accept();
        NioConnection conn = null;
        if (client_channel == null)
            return; // can happen if no connection is available to accept
        try {
            //创建连接对象
            conn = new NioConnection(client_channel, NioServer.this);
            //将连接对象注册到多路选择器上
            SelectionKey client_key = client_channel.register(selector, SelectionKey.OP_READ, conn);
            //在连接对象上注册选择键值对象
            conn.key(client_key); // we need to set the selection key of the client channel *not* the server channel
            //获取连接的对端地址
            Address peer_addr = conn.peerAddress();
            if (use_peer_connections)
                return;

            synchronized (this) {
                //替换掉旧的连接
                replaceConnection(peer_addr, conn); // closes old conn
                //启动连接对象
                conn.start();
                log.trace("%s: accepted connection from %s", local_addr, peer_addr);
            }
        } catch (Throwable ex) {
            Util.close(conn);
            removeConnectionIfPresent(conn.peerAddress(), conn);
            throw ex;
        }
    }

    // TODO: 17/5/25 by zmyer
    @Override
    @ManagedOperation(description = "Starts the server")
    public synchronized void start() throws Exception {
        if (running.compareAndSet(false, true)) {
            acceptor.start();
            super.start();
        }
    }

    // TODO: 17/5/25 by zmyer
    @Override
    @ManagedOperation(description = "Stops the server")
    public synchronized void stop() {
        super.stop();
        if (running.compareAndSet(true, false))
            Util.close(selector, channel); // closing the selector also stops the acceptor thread
    }

}
