package org.jgroups.protocols;

/**
 * A bundler based on a lockless ring buffer
 * @author Bela Ban
 * @since  4.0
 */

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.Util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

/**
 * This bundler adds all (unicast or multicast) messages to a ring buffer until max size has been exceeded, but does
 * send messages immediately when no other messages are available. If no space is available, a message by a sender
 * thread is simply dropped, as it will get retransmitted anyway. This makes this implementation completely non-blocking.
 * https://issues.jboss.org/browse/JGRP-1540
 */
public class RingBufferBundlerLockless extends BaseBundler implements Runnable {
    protected Message[]                   buf;
    protected int                         read_index;
    protected volatile int                write_index=0;
    protected final AtomicInteger         tmp_write_index=new AtomicInteger(0);
    protected final AtomicInteger         write_permits; // number of permits to write tmp_write_index
    protected final AtomicInteger         size=new AtomicInteger(0); // number of messages to be read: read_index + count == write_index
    protected final AtomicInteger         num_threads=new AtomicInteger(0); // number of threads currently in send()
    protected final AtomicLong            accumulated_bytes=new AtomicLong(0); // total number of bytes of unread msgs
    protected final AtomicBoolean         unparking=new AtomicBoolean(false);

    protected volatile Thread             bundler_thread;
    protected volatile boolean            running=true;
    protected int                         num_spins=40; // number of times we call Thread.yield before acquiring the lock (0 disables)
    protected static final String         THREAD_NAME="RingBufferBundlerLockless";
    protected BiConsumer<Integer,Integer> wait_strategy=SPIN_PARK;

    protected static final BiConsumer<Integer,Integer> SPIN=(it,spins) -> {;};
    protected static final BiConsumer<Integer,Integer> YIELD=(it,spins) -> Thread.yield();
    protected static final BiConsumer<Integer,Integer> PARK=(it,spins) -> LockSupport.parkNanos(1);
    protected static final BiConsumer<Integer,Integer> SPIN_PARK=(it, spins) -> {
        if(it < spins/10)
            ; // spin for the first 10% of all iterations, then switch to park()
        LockSupport.parkNanos(1);
    };
    protected static final BiConsumer<Integer,Integer> SPIN_YIELD=(it, spins) -> {
        if(it < spins/10)
            ;           // spin for the first 10% of the total number of iterations
        Thread.yield(); //, then switch to yield()
    };


    public RingBufferBundlerLockless() {
        this(1024);
    }


    public RingBufferBundlerLockless(int capacity) {
        buf=new Message[Util.getNextHigherPowerOfTwo(capacity)]; // for efficient % (mod) op
        this.write_permits=new AtomicInteger(buf.length);
    }

    public int                       readIndex()             {return read_index;}
    public int                       writeIndex()            {return write_index;}
    public Thread                    getThread()             {return bundler_thread;}
    public int                       getBufferSize()         {return size.get();}
    public int                       numSpins()              {return num_spins;}
    public RingBufferBundlerLockless numSpins(int n)         {num_spins=n; return this;}
    public String                    waitStrategy()          {return print(wait_strategy);}
    public RingBufferBundlerLockless waitStrategy(String st) {wait_strategy=createWaitStrategy(st, YIELD); return this;}


    public void init(TP transport) {
        super.init(transport);
    }

    public synchronized void start() {
        if(running)
            stop();
        bundler_thread=transport.getThreadFactory().newThread(this, THREAD_NAME);
        // bundler_thread.setPriority(Thread.MAX_PRIORITY);
        running=true;
        bundler_thread.start();
    }

    public synchronized void stop() {
        _stop(true);
    }

    public synchronized void stopAndFlush() {
        _stop(false);
    }

    public void send(Message msg) throws Exception {
        if(!running)
            return;

        num_threads.incrementAndGet();

        int tmp_index=getTmpIndex(); // decrements write_permits
        // System.out.printf("[%d] tmp_index=%d\n", Thread.currentThread().getId(), tmp_index);
        if(tmp_index == -1) { //todo: spin then block on not_full condition
            System.err.printf("buf is full (num_permits: %d, bundler: %s)\n", write_permits.get(), toString()); //todo: change to log stmt
            // LockSupport.unpark(bundler_thread);
            num_threads.decrementAndGet();
            return;
        }

        buf[tmp_index]=msg;
        long acc_bytes=accumulated_bytes.addAndGet(msg.size());

        //System.out.printf("[%d] acc_bytes=%d, num_threads=%d\n",
          //                Thread.currentThread().getId(), accumulated_bytes.get(), num_threads.get());

        int current_threads=num_threads.decrementAndGet();
        //System.out.printf("[%d] acc_bytes=%d, current_threads=%d\n",
          //                Thread.currentThread().getId(), accumulated_bytes.get(), current_threads);

        boolean no_other_threads=current_threads == 0;

        boolean unpark=(acc_bytes >= transport.getMaxBundleSize() && accumulated_bytes.compareAndSet(acc_bytes, 0))
          ||  no_other_threads;


        // only 2 threads at a time should do this (1st cond and 2nd cond), so we have to reduce this to
        // 1 thread as advanceWriteIndex() is not thread safe
        if(unpark && unparking.compareAndSet(false, true)) {

            int num_advanced=advanceWriteIndex();
            size.addAndGet(num_advanced);

            // System.out.printf("** [%d] acc_bytes=%d, no_other_threads=%b, advanced write-index by %d, bundler=%s\n",
            //                 Thread.currentThread().getId(), acc_bytes, no_other_threads, num_advanced, toString());

            //System.out.printf("[%d] advanced write index by %d messages to %d: tmp_index: %d\n",
            //                Thread.currentThread().getId(), num_advanced, write_index.get(), tmp_write_index.get());

            if(num_advanced > 0)
                LockSupport.unpark(bundler_thread);
            unparking.set(false);
        }
    }

    public void run() {
        while(running) {
            try {
                readMessages();
            }
            catch(Throwable t) {
            }
        }
    }


    protected int getTmpIndex() {
        int permit=getPermitToWrite();
        if(permit < 0)
            return -1;

        // here we're guaranteed to have space available for writing, we now need to find the right one
        for(;;) {
            int curr_tmp=tmp_write_index.get();
            int next_index=index(curr_tmp +1);
            if(tmp_write_index.compareAndSet(curr_tmp, next_index))
                return curr_tmp;
        }
    }

    protected int getPermitToWrite() {
        for(;;) {
            int available=write_permits.get();
            int remaining=available-1;
            if(remaining < 0 || write_permits.compareAndSet(available, remaining))
                return remaining;
        }
    }


    // Advance write_index up to tmp_write_index as long as no null msg is found
    protected int advanceWriteIndex() {
        int num=0, start=write_index;
        for(;;) {
            if(buf[start] == null)
                break;
            num++;
            start=index(start+1);
            if(start == tmp_write_index.get())
                break;
        }
        write_index=start;
        return num;
    }



    protected void readMessages() throws InterruptedException {
        int available_msgs=size.get();
        if(available_msgs > 0) {
            int sent_msgs=sendBundledMessages(buf, read_index, available_msgs);
            read_index=index(read_index + sent_msgs);
            size.addAndGet(-sent_msgs);
            write_permits.addAndGet(sent_msgs);
            // todo: if writers are blocked -> signalAll() on a not_full condition
        }
        LockSupport.park();
    }



    /** Read and send messages in range [read-index .. read-index+available_msgs-1] */
    public int sendBundledMessages(final Message[] buf, final int read_index, int available_msgs) {
        int       max_bundle_size=transport.getMaxBundleSize();
        byte[]    cluster_name=transport.cluster_name.chars();
        int       start=read_index;
        int       sent_msgs=0;

        while(available_msgs > 0) {
            Message msg=buf[start];
            if(msg == null) {
                start=increment(start);
                available_msgs--;
                continue;
            }

            Address dest=msg.dest();
            try {
                output.position(0);
                Util.writeMessageListHeader(dest, msg.src(), cluster_name, 1, output, dest == null);

                // remember the position at which the number of messages (an int) was written, so we can later set the
                // correct value (when we know the correct number of messages)
                int size_pos=output.position() - Global.INT_SIZE;
                int num_msgs=marshalMessagesToSameDestination(dest, buf, start, available_msgs, max_bundle_size);
                sent_msgs+=num_msgs;
                int current_pos=output.position();
                output.position(size_pos);
                output.writeInt(num_msgs);
                output.position(current_pos);
                transport.doSend(output.buffer(), 0, output.position(), dest);
            }
            catch(Exception ex) {
                log.error("failed to send message(s)", ex);
            }

            available_msgs--;
            start=increment(start);
        }
        return sent_msgs;
    }

    public String toString() {
        return String.format("read-index=%d write-index=%d size=%d cap=%d", read_index, write_index, size.get(), buf.length);
    }

    public int _readMessages() throws InterruptedException {
        int available_msgs=size.get();
        if(available_msgs > 0) {
            int sent_msgs=sendBundledMessages(buf, read_index, available_msgs);
            read_index=index(read_index + sent_msgs);
            size.addAndGet(-sent_msgs);
            write_permits.addAndGet(sent_msgs);
            // todo: if writers are blocked -> signalAll() on a not_full condition
            return sent_msgs;
        }
        return 0;
    }


    // Iterate through the following messages and find messages to the same destination (dest) and write them to output
    protected int marshalMessagesToSameDestination(Address dest, Message[] buf,
                                                   int start_index, int available_msgs, int max_bundle_size) throws Exception {
        int num_msgs=0, bytes=0;
        while(available_msgs > 0) {
            Message msg=buf[start_index];
            if(msg != null && Objects.equals(dest, msg.dest())) {
                long msg_size=msg.size();
                if(bytes + msg_size > max_bundle_size)
                    break;
                bytes+=msg_size;
                num_msgs++;
                buf[start_index]=null;
                msg.writeToNoAddrs(msg.src(), output, transport.getId());
            }
            available_msgs--;
            start_index=increment(start_index);
        }
        return num_msgs;
    }

    protected final int increment(int index) {return index+1 == buf.length? 0 : index+1;}
    protected final int index(int idx)     {return idx & (buf.length-1);}    // fast equivalent to %

    protected void _stop(boolean clear_queue) {
        running=false;
        Thread tmp=bundler_thread;
        bundler_thread=null;
        if(tmp != null) {
            tmp.interrupt();
            if(tmp.isAlive()) {
                try {tmp.join(500);} catch(InterruptedException e) {}
            }
        }
        if(clear_queue) {
            read_index=0;
            write_index=0;
            tmp_write_index.set(0);
            size.set(0);
        }
    }

    protected static String print(BiConsumer<Integer,Integer> wait_strategy) {
        if(wait_strategy      == null)            return null;
        if(wait_strategy      == SPIN)            return "spin";
        else if(wait_strategy == YIELD)           return "yield";
        else if(wait_strategy == PARK)            return "park";
        else if(wait_strategy == SPIN_PARK)       return "spin-park";
        else if(wait_strategy == SPIN_YIELD)      return "spin-yield";
        else return wait_strategy.getClass().getSimpleName();
    }

    protected BiConsumer<Integer,Integer> createWaitStrategy(String st, BiConsumer<Integer,Integer> default_wait_strategy) {
        if(st == null) return default_wait_strategy != null? default_wait_strategy : null;
        switch(st) {
            case "spin":            return wait_strategy=SPIN;
            case "yield":           return wait_strategy=YIELD;
            case "park":            return wait_strategy=PARK;
            case "spin_park":
            case "spin-park":       return wait_strategy=SPIN_PARK;
            case "spin_yield":
            case "spin-yield":      return wait_strategy=SPIN_YIELD;
            default:
                try {
                    Class<BiConsumer<Integer,Integer>> clazz=Util.loadClass(st, this.getClass());
                    return clazz.newInstance();
                }
                catch(Throwable t) {
                    log.error("failed creating wait_strategy " + st, t);
                    return default_wait_strategy != null? default_wait_strategy : null;
                }
        }
    }

    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}
