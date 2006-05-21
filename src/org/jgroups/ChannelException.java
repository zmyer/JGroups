// $Id: ChannelException.java,v 1.6.4.1 2006/05/21 09:36:58 mimbert Exp $

package org.jgroups;

/**
 * This class represents the super class for all exception types thrown by
 * JGroups.
 */
public class ChannelException extends Exception {

    public ChannelException() {
        super();
    }

    public ChannelException(String reason) {
        super(reason);
    }

    public ChannelException(String reason, Throwable cause) {
        super(reason + " - Cause = " + cause.getMessage());
    }

}
