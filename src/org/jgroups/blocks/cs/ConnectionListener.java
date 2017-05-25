package org.jgroups.blocks.cs;

/**
 * @author Bela Ban
 * @since 3.6.5
 */

// TODO: 17/5/25 by zmyer
public interface ConnectionListener {
    void connectionClosed(Connection conn, String reason);

    void connectionEstablished(Connection conn);
}
