package org.jgroups;

/**
 * Defines the callbacks that are invoked when messages, views etc are received on a channel
 *
 * @author Bela Ban
 * @see JChannel#setReceiver(Receiver)
 * @since 2.0
 */
// TODO: 17/7/4 by zmyer
public interface Receiver extends MessageListener, MembershipListener {
}
