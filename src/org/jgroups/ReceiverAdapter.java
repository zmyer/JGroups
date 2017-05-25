package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;
import org.jgroups.util.MessageBatch;

/**
 * An adapter implementing the Receiver interface with no-op implementations. When implementing a
 * callback, we can simply extend ReceiverAdapter and overwrite receive() in order to not having to
 * implement all callbacks of the interface.
 *
 * @author Bela Ban
 * @since 2.0
 */
public class ReceiverAdapter implements Receiver {

    public void receive(Message msg) {
    }

    public void receive(MessageBatch batch) {
        for (Message msg : batch) {
            try {
                receive(msg);
            } catch (Throwable t) {
            }
        }
    }

    public void getState(OutputStream output) throws Exception {
    }

    public void setState(InputStream input) throws Exception {
    }

    public void viewAccepted(View view) {
    }

    public void suspect(Address mbr) {
    }

    public void block() {
    }

    public void unblock() {
    }
}
