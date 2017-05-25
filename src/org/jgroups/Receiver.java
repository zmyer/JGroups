package org.jgroups;

/**
 * Defines the callbacks that are invoked when messages, views etc are received on a channel
 *
 * @author Bela Ban
 * @see JChannel#setReceiver(Receiver)
 * @since 2.0
 */
public interface Receiver extends MessageListener, MembershipListener {
}
