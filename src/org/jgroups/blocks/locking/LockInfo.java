package org.jgroups.blocks.locking;

import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 */
// TODO: 17/7/4 by zmyer
public class LockInfo {
    protected final String name;
    private final boolean is_trylock;
    private final boolean lock_interruptibly;
    private final boolean use_timeout;
    protected final long timeout;
    private final TimeUnit time_unit;

    LockInfo(String name, boolean is_trylock, boolean lock_interruptibly,
        boolean use_timeout, long timeout, TimeUnit time_unit) {
        this.name = name;
        this.is_trylock = is_trylock;
        this.lock_interruptibly = lock_interruptibly;
        this.use_timeout = use_timeout;
        this.timeout = timeout;
        this.time_unit = time_unit;
    }

    public boolean isTrylock() {
        return is_trylock;
    }

    public boolean isLockInterruptibly() {
        return lock_interruptibly;
    }

    public boolean isUseTimeout() {
        return use_timeout;
    }

    public String getName() {
        return name;
    }

    public long getTimeout() {
        return timeout;
    }

    public TimeUnit getTimeUnit() {
        return time_unit;
    }

    public String toString() {
        return name + ", trylock=" + is_trylock + ", timeout=" + timeout;
    }
}

