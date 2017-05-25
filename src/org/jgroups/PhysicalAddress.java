package org.jgroups;

import org.jgroups.util.UUID;

/**
 * Represents a physical (as opposed to logical) address
 *
 * @author Bela Ban
 * @see UUID
 * @since 2.6
 */
// TODO: 17/5/25 by zmyer
public interface PhysicalAddress extends Address {
    String printIpAddress();
}
