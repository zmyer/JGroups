package org.jgroups.stack;

import org.jgroups.Address;

/**
 * Callback to provide custom addresses. Will be called by {@link org.jgroups.JChannel#connect(String)}.
 *
 * @author Bela Ban
 * @since 2.12
 */
// TODO: 17/7/5 by zmyer
public interface AddressGenerator {
    Address generateAddress();
}
