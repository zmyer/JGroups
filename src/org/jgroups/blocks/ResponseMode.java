package org.jgroups.blocks;

/**
 * Enum capturing the various response modes for RPCs
 *
 * @author Bela Ban
 * @since 3.0
 */
// TODO: 17/7/4 by zmyer
public enum ResponseMode {
    /** Returns the first response received */
    GET_FIRST,

    /** return all responses */
    GET_ALL,

    /** return no response (async call) */
    GET_NONE
}
