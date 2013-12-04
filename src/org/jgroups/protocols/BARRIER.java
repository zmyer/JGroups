package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * All messages up the stack have to go through a barrier (read lock, RL). By default, the barrier is open.
 * When a CLOSE_BARRIER event is received, we close the barrier by acquiring a write lock (WL). This succeeds when all
 * previous messages have completed (by releasing their RLs). Thus, when we have acquired the WL, we know that there
 * are no pending messages processed.<br/>
 * When an OPEN_BARRIER event is received, we simply open the barrier again and let all messages pass in the up
 * direction. This is done by releasing the WL.
 * @author Bela Ban
 */
@MBean(description="Blocks all multicast threads when closed")
public class BARRIER extends Protocol {
    
    @Property(description="Max time barrier can be closed. Default is 60000 ms")
    protected long                 max_close_time=60000; // how long can the barrier stay closed (in ms) ? 0 means forever


    protected final Lock           lock=new ReentrantLock();
    protected final AtomicBoolean  barrier_closed=new AtomicBoolean(false);

    /** signals to waiting threads that the barrier is open again */
    protected Condition            barrier_opened=lock.newCondition();
    protected Condition            no_msgs_pending=lock.newCondition();
    protected Map<Thread, Object>  in_flight_threads=Util.createConcurrentMap();
    protected Future<?>            barrier_opener_future=null;
    protected TimeScheduler        timer;
    protected Address              local_addr;
    protected static final Object  NULL=new Object();
    // mbrs from which unicasts should be accepted even if BARRIER is closed (PUNCH_HOLE adds, CLOSE_HOLE removes mbrs)
    protected final Set<Address>   holes=new HashSet<Address>();


    @ManagedAttribute(description="Shows whether the barrier closed")
    public boolean isClosed() {
        return barrier_closed.get();
    }

    @ManagedAttribute(description="Lists the members whose unicast messages are let through")
    public String getHoles() {return holes.toString();}

    public int getNumberOfInFlightThreads() {
        return in_flight_threads.size();
    }

    @ManagedAttribute
    public int getInFlightThreadsCount() {
        return getNumberOfInFlightThreads();
    }

    @ManagedAttribute
    public boolean isOpenerScheduled() {
        return barrier_opener_future != null && !barrier_opener_future.isDone() && !barrier_opener_future.isCancelled();
    }

    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
    }

    public void stop() {
        super.stop();
        openBarrier();
    }


    public void destroy() {
        super.destroy();
        openBarrier();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.CLOSE_BARRIER:
                closeBarrier();
                return null;
            case Event.OPEN_BARRIER:
                openBarrier();
                return null;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.PUNCH_HOLE:
                Address mbr=(Address)evt.getArg();
                holes.add(mbr);
                return null;
            case Event.CLOSE_HOLE:
                mbr=(Address)evt.getArg();
                holes.remove(mbr);
                return null;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.getDest() != null) // https://issues.jboss.org/browse/JGRP-1341: let unicast messages pass
                    if((msg.isFlagSet(Message.Flag.OOB) && msg.isFlagSet(Message.Flag.INTERNAL)) || holes.contains(msg.getSrc()))
                        return up_prot.up(evt);
                Thread current_thread=Thread.currentThread();
                blockIfBarrierClosed(current_thread);
                try {
                    return up_prot.up(evt);
                }
                finally {
                    unblock(current_thread);
                }
            case Event.CLOSE_BARRIER:
                closeBarrier();
                return null;
            case Event.OPEN_BARRIER:
                openBarrier();
                return null;
        }
        return up_prot.up(evt);
    }




    public void up(MessageBatch batch) {
        if(batch.dest() != null) { // let unicast message batches pass
            if((batch.mode() == MessageBatch.Mode.OOB && batch.mode() == MessageBatch.Mode.INTERNAL) || holes.contains(batch.sender())) {
                up_prot.up(batch);
                return;
            }
        }
        Thread current_thread=Thread.currentThread();
        blockIfBarrierClosed(current_thread);
        try {
            up_prot.up(batch);
        }
        finally {
            unblock(current_thread);
        }
    }


    protected void blockIfBarrierClosed(final Thread current_thread) {
        in_flight_threads.put(current_thread, NULL);
        if(barrier_closed.get()) {
            lock.lock();
            try {
                // Feb 28 2008 (Gray Watson): remove myself because barrier is closed
                in_flight_threads.remove(current_thread);
                while(barrier_closed.get()) {
                    try {
                        barrier_opened.await();
                    }
                    catch(InterruptedException e) {
                    }
                }
            }
            finally {
                // Feb 28 2008 (Gray Watson): barrier is now open, put myself back in_flight
                in_flight_threads.put(current_thread, NULL);
                lock.unlock();
            }
        }
    }

    protected void unblock(final Thread current_thread) {
        if(in_flight_threads.remove(current_thread) == NULL && barrier_closed.get() && in_flight_threads.isEmpty()) {
            lock.lock();
            try {
                no_msgs_pending.signalAll();
            }
            finally {
                lock.unlock();
            }
        }
    }

    /** Close the barrier. Temporarily remove all threads which are waiting or blocked, re-insert them after the call */
    protected void closeBarrier() {
        if(!barrier_closed.compareAndSet(false, true))
            return; // barrier was already closed

        Set<Thread> threads=new HashSet<Thread>();

        lock.lock();
        try {
            // wait until all pending (= in-progress, runnable threads) msgs have returned
            in_flight_threads.remove(Thread.currentThread());
            while(!in_flight_threads.isEmpty()) {
                for(Iterator<Thread> it=in_flight_threads.keySet().iterator(); it.hasNext();) {
                    Thread thread=it.next();
                    Thread.State state=thread.getState();
                    if(state != Thread.State.RUNNABLE && state != Thread.State.NEW) {
                        threads.add(thread);
                        it.remove();
                    }
                }
                if(!in_flight_threads.isEmpty()) {
                    try {
                        no_msgs_pending.await(1000, TimeUnit.MILLISECONDS);
                    }
                    catch(InterruptedException e) {
                    }
                }
            }
        }
        finally {
            for(Thread thread: threads)
                in_flight_threads.put(thread, NULL);
            lock.unlock();
        }

        if(log.isTraceEnabled())
            log.trace("barrier was closed");

        if(max_close_time > 0)
            scheduleBarrierOpener();
    }

    @ManagedOperation(description="Opens the barrier. No-op if already open")
    public void openBarrier() {
        lock.lock();
        try {
            if(!barrier_closed.compareAndSet(true, false))
                return; // barrier was already open
            barrier_opened.signalAll();
        }
        finally {
            lock.unlock();
        }
        if(log.isTraceEnabled())
            log.trace("barrier was opened");
        cancelBarrierOpener(); // cancels if running
    }

    private void scheduleBarrierOpener() {
        if(barrier_opener_future == null || barrier_opener_future.isDone()) {
            barrier_opener_future=timer.schedule(new Runnable() {public void run() {openBarrier();}},
                                                 max_close_time, TimeUnit.MILLISECONDS
            );
        }
    }

    private void cancelBarrierOpener() {
        if(barrier_opener_future != null) {
            barrier_opener_future.cancel(true);
            barrier_opener_future=null;
        }
    }
}
