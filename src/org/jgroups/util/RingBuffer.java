package org.jgroups.util;

import java.util.Collection;
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
    protected int                                        ri, wi;   // read and write indices
    protected int                                        count;    // number of elements available to be read
    protected final Lock                                 lock=new ReentrantLock();
    protected final java.util.concurrent.locks.Condition not_empty=lock.newCondition(); // reader can block on this
    protected final java.util.concurrent.locks.Condition not_full=lock.newCondition();  // writes can block on this

    public RingBuffer(int capacity) {
        int c=Util.getNextHigherPowerOfTwo(capacity); // power of 2 for faster mod operation
        buf=(T[])new Object[c];
    }

    public int capacity() {return buf.length;}

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

    public int count() {
        lock.lock();
        try {
            return count;
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
    public RingBuffer<T> put(T element) throws InterruptedException {
        if(element == null)
            return this;
        lock.lock();
        try {
            while(count == buf.length)
                not_full.await();

            buf[wi]=element;
            if(++wi == buf.length)
                wi=0;
            count++;
            not_empty.signal();
            return this;
        }
        finally {
            lock.unlock();
        }
    }


    public T take() throws InterruptedException {
        lock.lock();
        try {
            while(count == 0)
                not_empty.await();
            T el=buf[ri];
            buf[ri]=null;
            if(++ri == buf.length)
                ri=0;
            count--;
            not_full.signal();
            return el;
        }
        finally {
            lock.unlock();
        }
    }

    public int drainTo(Collection<T> list) {
        return drainTo(list, Integer.MAX_VALUE);
    }

    public int drainTo(Collection<T> list, int max_elements) {
        try {
            return drainTo(list, max_elements, false);
        }
        catch(InterruptedException e) {
            return 0;
        }
    }

    public int drainTo(Collection<T> list, int max_elements, boolean block) throws InterruptedException {
        if(list == null)
            return 0;
        int cnt=0;
        lock.lock();
        try {
            if(block) {
                while(count == 0)
                    not_empty.await();
            }
            int num_elements=Math.min(max_elements, count);
            for(int i=0; i < num_elements; i++) {
                T el=buf[ri];
                list.add(el);
                buf[ri]=null;
                if(++ri == buf.length)
                    ri=0;
                cnt++;
            }
            if(cnt > 0) {
                count-=cnt;
                not_full.signal();
            }
            return cnt;
        }
        finally {
            lock.unlock();
        }
    }

    public int drainToLockless(Collection<T> list, int max_elements, boolean block, int num_spins) throws InterruptedException {
        if(list == null)
            return 0;
        int cnt=0;

        if(block) {

            // try spinning first (experimental)
            for(int i=0; i < num_spins; i++) {
                if(count > 0)
                    break;
                Thread.yield();
            }

            if(count == 0) {
                lock.lock();
                try {
                    while(count == 0)
                        not_empty.await();
                }
                finally {
                    lock.unlock();
                }
            }
        }

        int num_elements=Math.min(max_elements, count);
        int read_index=ri;

        for(int i=0; i < num_elements; i++) {
            T el=buf[read_index];
            list.add(el);
            buf[read_index]=null;
            if(++read_index == buf.length)
                read_index=0;
            cnt++;
        }
        if(cnt > 0) {
            lock.lock();
            try {
                ri=read_index;
                count-=cnt;
                not_full.signal();
            }
            finally {
                lock.unlock();
            }
        }
        return cnt;
    }


    public RingBuffer<T> clear() {
        lock.lock();
        try {
            count=ri=wi=0;
            for(int i=0; i < buf.length; i++)
                buf[i]=null;
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return count;
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


    /** Apparently much more efficient than mod (%) */
    protected int realIndex(int index) {
        return index & (buf.length -1);
    }


}
