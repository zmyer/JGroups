package org.jgroups.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A condition variable with methods for (timed) waiting and signalling
 *
 * @author Bela Ban
 * @since 3.6
 */

// TODO: 17/5/25 by zmyer
public class CondVar {
    protected final Lock lock;
    protected final java.util.concurrent.locks.Condition cond;

    // TODO: 17/5/25 by zmyer
    public CondVar() {
        lock = new ReentrantLock();
        cond = lock.newCondition();
    }

    // TODO: 17/5/25 by zmyer
    public CondVar(final Lock lock) {
        this.lock = lock;
        cond = lock.newCondition();
    }

    /**
     * Blocks until condition is true.
     *
     * @param condition The condition. Must be non-null
     */
    // TODO: 17/5/25 by zmyer
    public void waitFor(Condition condition) {
        boolean intr = false;
        lock.lock();
        try {
            while (!condition.isMet()) {
                try {
                    cond.await();
                } catch (InterruptedException e) {
                    intr = true;
                }
            }
        } finally {
            lock.unlock();
            if (intr)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Blocks until condition is true or the time elapsed
     *
     * @param condition The condition
     * @param timeout The timeout to wait. A value <= 0 causes immediate return
     * @param unit TimeUnit
     * @return The condition's status
     */
    // TODO: 17/5/25 by zmyer
    public boolean waitFor(Condition condition, long timeout, TimeUnit unit) {
        boolean intr = false;
        final long timeout_ns = TimeUnit.NANOSECONDS.convert(timeout, unit);
        lock.lock();
        try {
            for (long wait_time = timeout_ns, start = System.nanoTime(); wait_time > 0 && !condition.isMet(); ) {
                try {
                    wait_time = cond.awaitNanos(wait_time);
                } catch (InterruptedException e) {
                    wait_time = timeout_ns - (System.nanoTime() - start);
                    intr = true;
                }
            }
            return condition.isMet();
        } finally {
            lock.unlock();
            if (intr)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Wakes up one (signal_all=false) or all (signal_all=true) blocked threads. Usually called when
     * the condition changed to true.
     *
     * @param signal_all
     */
    // TODO: 17/5/25 by zmyer
    public void signal(boolean signal_all) {
        lock.lock();
        try {
            if (signal_all)
                cond.signalAll();
            else
                cond.signal();
        } finally {
            lock.unlock();
        }
    }
}
