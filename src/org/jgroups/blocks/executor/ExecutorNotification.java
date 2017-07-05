package org.jgroups.blocks.executor;

/**
 * @author wburns
 */
// TODO: 17/7/4 by zmyer
public interface ExecutorNotification {
    void resultReturned(Object obj);

    void throwableEncountered(Throwable t);

    void interrupted(Runnable runnable);
}
