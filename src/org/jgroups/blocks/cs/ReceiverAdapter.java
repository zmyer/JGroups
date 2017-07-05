package org.jgroups.blocks.cs;

import java.io.DataInput;
import java.nio.ByteBuffer;
import org.jgroups.Address;
import org.jgroups.util.Util;

/**
 * An impl of {@link Receiver}. Will get removed with the switch to Java 8; instead we'll use a
 * default impl in Receiver
 *
 * @author Bela Ban
 * @since 3.6.5
 */

// TODO: 17/5/25 by zmyer
public class ReceiverAdapter implements Receiver {
    public void receive(Address sender, byte[] buf, int offset, int length) {

    }

    /**
     * The default implementation assumes that {@link ByteBuffer#flip()}  or {@link
     * ByteBuffer#rewind()} was called on buf before invoking this callback
     *
     * @param sender message sender
     * @param buf byte buffer
     */
    // TODO: 17/5/25 by zmyer
    public void receive(Address sender, ByteBuffer buf) {
        Util.bufferToArray(sender, buf, this);
    }

    public void receive(Address sender, DataInput in) throws Exception {

    }
}
