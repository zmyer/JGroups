package org.jgroups.protocols.relay;

import org.jgroups.Address;

/**
 * Address with a site suffix
 *
 * @author Bela Ban
 * @since 3.2
 */
// TODO: 17/7/5 by zmyer
public interface SiteAddress extends Address {
    /** Returns the ID of the site (all sites need to have a unique site ID) */
    String getSite();
}
