
package org.jgroups;

/**
 * Thrown if a message is sent to a suspected member.
 *
 * @author Bela Ban
 * @since 2.0
 */
// TODO: 17/7/6 by zmyer
public class SuspectedException extends Exception {

    private static final long serialVersionUID = -6663279911010545655L;

    public SuspectedException() {
    }

    public SuspectedException(Object suspect) {
    }

    public String toString() {
        return "SuspectedException";
    }
}
