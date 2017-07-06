package org.jgroups;

/**
 * <code>StateTransferException</code> is thrown to indicate a failure of a state transfer between
 * cluster members.
 * <p>
 *
 * @author Vladimir Blagojevic
 * @since 2.6
 */
// TODO: 17/7/6 by zmyer
public class StateTransferException extends Exception {
    private static final long serialVersionUID = -4070956583392020498L;

    public StateTransferException() {
    }

    StateTransferException(String reason) {
        super(reason);
    }

    StateTransferException(String reason, Throwable cause) {
        super(reason, cause);
    }

}
