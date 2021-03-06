package org.jgroups.blocks.cs;

import java.io.Closeable;
import java.nio.ByteBuffer;
import org.jgroups.Address;

/**
 * Represents a connection to a peer
 */

// TODO: 17/5/25 by zmyer
public abstract class Connection implements Closeable {
    static final byte[] cookie = {'b', 'e', 'l', 'a'};
    Address peer_addr;    // address of the 'other end' of the connection
    long last_access;  // timestamp of the last access to this connection (read or write)

    abstract public boolean isOpen();

    abstract public boolean isConnected();

    abstract public Address localAddress();

    abstract public Address peerAddress();

    abstract public boolean isExpired(long millis);

    abstract public void connect(Address dest) throws Exception;

    abstract public void start() throws Exception;

    abstract public void send(byte[] buf, int offset, int length) throws Exception;

    abstract public void send(ByteBuffer buf) throws Exception;
}
