package org.jgroups.util;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Attempt at writing a fast transfer queue, which is bounded. The take() method blocks until there is an element, but
 * the offer() method drops the element and returns if the queue is full (doesn't block). <p/>
 * The design assumes a number of producers but only <em>one</em> consumer. The consumer only blocks when the queue is
 * empty (on the not-empty condition), the producers block when the queue is full (on the not-full condition). The
 * producers increment a count atomically and if the count is greater than the capacity, they block on the not-full
 * condition. The consumer decrements the condition and signals the not-full condition when the count is capacity -1
 * (from capacity to capacity-1).
 * The producers signal not-empty when the count is 1 (from 0 to 1)
 *
 * @author  Bela Ban
 * @since   3.5
 */
public class ConcurrentLinkedBlockingQueue2<T> extends ConcurrentLinkedQueue<T> implements BlockingQueue<T> {
    private static final long     serialVersionUID=7094985083335772755L;
    protected final int           capacity;
    protected final AtomicInteger count=new AtomicInteger(0);

    // not_empty is signalled by a producer when count went from 0 to 1
    protected final Lock          not_empty_lock=new ReentrantLock();
    protected final Condition     not_empty=not_empty_lock.newCondition();

    // not_full is signalled when the count goes from capacity to capacity-1
    protected final Lock          not_full_lock=new ReentrantLock();
    protected final Condition     not_full=not_full_lock.newCondition();



    public ConcurrentLinkedBlockingQueue2(int capacity) {
        this.capacity=capacity;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("** num_awaits=" + not_empty_awaits + ", not_full_awaits=" + not_full_awaits);
            }
        });
    }


    int not_empty_awaits=0;
    AtomicInteger not_full_awaits=new AtomicInteger(0);

    
    /**
     * Drops elements if capacity has been reached. That's OK for the ThreadPoolExecutor as dropped messages
     * will get retransmitted
     * @param t
     * @return
     */
    public boolean offer(T t) {
        return false;
    }

    public T take() throws InterruptedException {
        waitForNotEmpty();

        // at this stage, we are guaranteed to have a value
        T val=super.poll();
        decrCount();
        return val;
    }



    public T poll() {
        T val=super.poll();
        if(val != null)
            decrCount();
        return val;
    }

    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    public boolean remove(Object o) {
        return super.remove(o);
    }

    public int remainingCapacity() {
        return capacity - size();
    }

    public int drainTo(Collection<? super T> c) {
        int cnt=0;
        if(c == null)
            return cnt;

        for(;;) {
            T el=poll();
            if(el == null)
                break;
            c.add(el);
            cnt++;
        }

        return cnt;
    }

    
    public void put(T t) throws InterruptedException {
        waitForNotFull();
        super.offer(t);
        incrCount();
    }

    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(t);
    }

    public int size() {
        return count.get();
    }

    public int drainTo(Collection<? super T> c, int maxElements) {
        return drainTo(c);
    }


    protected void waitForNotEmpty() throws InterruptedException {
        while(count.get() == 0) {
            not_empty_lock.lock();
            try {
                System.out.println("-----> waiting for not empty: num_awaits=" + ++not_empty_awaits + ", count=" + count);
                not_empty.await();
            }
            finally {
                not_empty_lock.unlock();
            }
        }
    }

    protected void waitForNotFull() throws InterruptedException {
        while(count.get() >= capacity) {
            not_full_lock.lock();
            try {
                not_full_awaits.incrementAndGet();
                not_full.await();
            }
            finally {
                not_full_lock.unlock();
            }
        }
    }

    protected void decrCount() {
        int prev_count=count.getAndDecrement();
        // if(prev_count == capacity) {
        if(prev_count >= capacity) {
            not_full_lock.lock();
            try {
               // not_full.signalAll();
                not_full.signal();
            }
            finally {
                not_full_lock.unlock();
            }
        }
    }

    protected void incrCount() {
        int prev_count=count.getAndIncrement();
        if(prev_count == 0) {
            not_empty_lock.lock();
            try {
                not_empty.signal(); // not signalAll() as there is only *one* consumer !
            }
            finally {
                not_empty_lock.unlock();
            }
        }
    }
}
