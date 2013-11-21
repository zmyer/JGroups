package org.jgroups.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A queue with many producers and consumers. However, only <em>one</em> consumer gets to remove and process elements
 * at any given time. This is done through the use of locks.
 * @author Bela Ban
 * @since  3.5
 */
public class ProcessingQueue<T> {
    protected final java.util.Queue<T>  queue=new ConcurrentLinkedQueue<T>();
    protected final AtomicInteger       added=new AtomicInteger(0), removed=new AtomicInteger(0);
    protected final ReentrantLock       producer_lock=new ReentrantLock(), consumer_lock=new ReentrantLock();
    protected int                       count=0;
    protected Handler<T>                handler;


    public java.util.Queue<T> getQueue()   {return queue;}
    public int                size()       {return queue.size();}
    public int                getAdded()   {return added.get();}
    public int                getRemoved() {return removed.get();}

    public ProcessingQueue<T> setHandler(Handler<T> handler) {this.handler=handler; return this;}

    public void add(T element) {
        producer_lock.lock();
        try {
            queue.add(element);
            count++;
            added.incrementAndGet();
        }
        finally {
            producer_lock.unlock();
        }

        process();
    }

    protected void process() {
        if(consumer_lock.tryLock()) {
            try {
                while(true) {
                    T element=queue.poll();
                    if(element != null) {
                        removed.incrementAndGet();
                        if(handler != null) {
                            try {
                                handler.handle(element);
                            }
                            catch(Throwable t) {
                                t.printStackTrace(System.err);
                            }
                        }
                    }

                    producer_lock.lock();
                    try {
                        if(count == 0 || count-1 == 0) {
                            count=0;
                            consumer_lock.unlock();
                            return;
                        }
                        count--;
                    }
                    finally {
                        producer_lock.unlock();
                    }
                }
            }
            finally {
                if(consumer_lock.isHeldByCurrentThread())
                    consumer_lock.unlock();
            }
        }
    }

    public String toString() {
        return queue.toString();
    }


    public interface Handler<T> {
        void handle(T element);
    }
}
