package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Ring buffer of fixed capacity designed for multiple writers but only a single reader. Advancing the read or
 * write index blocks until it is possible to do so.
 * @author Bela Ban
 * @since  4.0
 */
public class RingBuffer<T> {
    protected final T[]                                  buf;
    protected int                                        ri, wi; // read and write indices
    protected final Lock                                 lock=new ReentrantLock();
    protected final java.util.concurrent.locks.Condition not_empty=lock.newCondition(); // reader can block on this
    protected final java.util.concurrent.locks.Condition not_full=lock.newCondition();  // writes can block on this

    public RingBuffer(int capacity) {
        int c=Util.getNextHigherPowerOfTwo(capacity); // power of 2 for faster mod operation
        buf=(T[])new Object[c];
    }

    public int readIndex() {
        lock.lock();
        try {
            return ri;
        }
        finally {
            lock.unlock();
        }
    }

    public int writeIndex() {
        lock.lock();
        try {
            return wi;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Tries to add a new element at the current write index and advances the write index. If the write index is at the
     * same position as the read index, this will block until the read index is advanced.
     * @param element the element to be added. Must not be null, or else this operation returns immediately without
     *                adding the null element
     */
    public RingBuffer write(T element) throws InterruptedException {
        if(element == null)
            return this;
        lock.lock();
        try {
            buf[wi]=element;
            advanceWriteIndex(); // tries to advance the write index, but blocks as long as wi+1 == ri
            return this;
        }
        finally {
            lock.unlock();
        }
    }


    public T read() throws InterruptedException {
        lock.lock();
        try {
            waitForData(); // blocks while ri == wi
            T el=buf[ri];
            advanceReadIndex();
            return el;
        }
        finally {
            lock.unlock();
        }
    }


    public int size() {
        lock.lock();
        try {
            int cnt=0, read_index=ri, write_index=wi;
            for(; read_index != write_index; read_index++, cnt++) {
                if(read_index == buf.length) // handle wrap-around. this should be faster than a mod operation
                    read_index=0;
             //    if(read_index == write_index)
                //    break;
            }
            return cnt;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return ri == wi;
        }
        finally {
            lock.unlock();
        }
    }

    public String toString() {
        return String.format("[ri=%d wi=%d size=%d cap=%d]", ri, wi, size(), buf.length);
    }

    /** Advances the write index and notifies a waiting reader */
    @GuardedBy("lock") protected void advanceWriteIndex() throws InterruptedException {
        // block if wi+1 == ri:
        int new_wi=wi+1;
        if(new_wi == buf.length)
            new_wi=0;

        while(new_wi == ri)
            not_full.await();

        if(++wi == buf.length)
            wi=0;
        not_empty.signal();
    }

    /** Advances the read index and notifies waiting writers */
    @GuardedBy("lock") protected void advanceReadIndex() {
        if(++ri == buf.length)
            ri=0;
        not_full.signal(); // todo: use signalAll() as we can have multiple writers?
    }

    @GuardedBy("lock") protected void waitForData() throws InterruptedException {
        while(ri == wi)
            not_empty.await();
    }

    /** Apparently much more efficient than mod (%) */
    protected int realIndex(int index) {
        return index & (buf.length -1);
    }

}
