
package org.jgroups.stack;

import org.jgroups.Address;

/**
 * Contains parameters for state transfer. Exchanged between channel and STATE_TRANSFER layer. The
 * state is retrieved from 'target'. If target is null, then the state will be retrieved from the
 * oldest member (usually the coordinator).
 *
 * @author Bela Ban
 */
// TODO: 17/7/6 by zmyer
public class StateTransferInfo {
    public Address target = null;
    public long timeout = 0;
    public byte[] state = null;

    public StateTransferInfo() {
    }

    public StateTransferInfo(Address target) {
        this.target = target;
    }

    public StateTransferInfo(Address target, long timeout) {
        this.target = target;
        this.timeout = timeout;
    }

    public StateTransferInfo(Address target, long timeout, byte[] state) {
        this.target = target;
        this.timeout = timeout;
        this.state = state;
    }

    public StateTransferInfo copy() {
        return new StateTransferInfo(target, timeout, state);
    }

    public String toString() {
        StringBuilder ret = new StringBuilder();
        ret.append("target=").append(target);
        if (state != null)
            ret.append(", state=").append(state.length).append(" bytes");
        ret.append(", timeout=").append(timeout);
        return ret.toString();
    }
}
