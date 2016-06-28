package org.jgroups.protocols;

/**
 * A bundler based on a lockless ring buffer
 * @author Bela Ban
 * @since  4.0
 */

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * This bundler adds all (unicast or multicast) messages to a ring buffer until max size has been exceeded, but does
 * send messages immediately when no other messages are available. If no space is available, a message by a sender
 * thread is simply dropped, as it will get retransmitted anyway. This makes this implementation completely non-blocking.
 * https://issues.jboss.org/browse/JGRP-1540
 */
public class RingBufferBundlerLockless2 extends BaseBundler {
    protected Message[]                   buf;
    protected final AtomicInteger         read_index=new AtomicInteger(0);
    protected final AtomicInteger         write_index=new AtomicInteger(1);
    protected final AtomicLong            accumulated_bytes=new AtomicLong(0);
    protected final AtomicInteger         num_threads=new AtomicInteger(0);

    protected static final String         THREAD_NAME=RingBufferBundlerLockless2.class.getSimpleName();
    //protected static final AtomicIntegerFieldUpdater write_updater;
    public static final Message           NULL_MSG=new Message(false);
    protected final AtomicBoolean         unparking=new AtomicBoolean(false);
    protected final BundlerThread         bundler_thread=new BundlerThread();

    // stats
    protected final AverageMinMax         num_tries_to_get_write_index=new AverageMinMax();
    protected final AtomicInteger         no_write_index=new AtomicInteger(0);
    protected final AtomicInteger         num_unparks=new AtomicInteger(0);
    protected final AtomicInteger         num_reads=new AtomicInteger(0);
    protected final AtomicInteger         num_unparks_by_size=new AtomicInteger(0);
    protected final AtomicInteger         num_unparks_by_threads=new AtomicInteger(0);



    public RingBufferBundlerLockless2() {
        this(1024);
    }


    public RingBufferBundlerLockless2(int capacity) {
        buf=new Message[Util.getNextHigherPowerOfTwo(capacity)]; // for efficient % (mod) op
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.printf("num_tries_to_get_write_index: %s, no_write_index=%d, num_unparks=%d, num_reads=%d\n" +
                                "num_unparks_size=%d, num_unparks_threads=%d\n",
                              num_tries_to_get_write_index, no_write_index.get(), num_unparks.get(), num_reads.get(),
                              num_unparks_by_size.get(), num_unparks_by_threads.get());
        }));
    }

    public int                        readIndex()             {return read_index.get();}
    public int                        writeIndex()            {return write_index.get();}
    public RingBufferBundlerLockless2 reset()                 {read_index.set(0); write_index.set(1); return this;}

    public int getBufferSize() {
        return _size(read_index.get(), write_index.get());
    }

    protected int _size(int ri, int wi) {
        return ri < wi? wi-ri-1 : buf.length - ri -1 +wi;
    }

    public void init(TP transport) {
        super.init(transport);
    }

    public synchronized void start() {
        bundler_thread.start();
    }

    public synchronized void stop() {
        bundler_thread.stop();
    }

    public synchronized void stopAndFlush() {
        bundler_thread.stop(false);
    }

    public void send(Message msg) throws Exception {
        if(msg == null)
            throw new IllegalArgumentException("message must not be null");

        num_threads.incrementAndGet();

        int tmp_write_index=getWriteIndex(read_index.get());
        // System.out.printf("[%d] tmp_write_index=%d\n", Thread.currentThread().getId(), tmp_write_index);
        if(tmp_write_index == -1) {
            System.err.printf("buf is full: %s\n", toString()); //todo: change to log stmt
            no_write_index.incrementAndGet();
            unparkIfNeeded(0);
            return;
        }

        buf[tmp_write_index]=msg;
        unparkIfNeeded(msg.size());
    }

    protected void unparkIfNeeded(long size) {
        long acc_bytes=size > 0? accumulated_bytes.addAndGet(size) : accumulated_bytes.get();
        boolean size_exceeded=acc_bytes >= transport.getMaxBundleSize() && accumulated_bytes.compareAndSet(acc_bytes, 0);
        boolean no_other_threads=num_threads.decrementAndGet() == 0;

        boolean unpark=size_exceeded || no_other_threads;

        // only 2 threads at a time should do this in parallel (1st cond and 2nd cond)
        if(unpark && unparking.compareAndSet(false, true)) {
            bundler_thread.unpark();

            num_unparks.incrementAndGet();
            if(size_exceeded) {
                num_unparks_by_size.incrementAndGet();
                // System.out.printf("** unparked because acc_bytes is %d (size=%d)\n", acc_bytes, size);
            }
            if(no_other_threads) {
                num_unparks_by_threads.incrementAndGet();
                // System.out.printf("** unparked because num_threads=0\n");
            }
            unparking.set(false);
        }
    }


    protected int getWriteIndex(int current_read_index) {
        int num_tries=0;
        try {
            for(;;) {
                int wi=write_index.get();
                int next_wi=index(wi + 1);
                if(next_wi == current_read_index)
                    return -1;
                if(write_index.compareAndSet(wi, next_wi)) {
                    num_tries++;
                    return wi;
                }
            }
        }
        finally {
            if(num_tries > 0)
                num_tries_to_get_write_index.add(num_tries);
        }
    }



    public int _readMessages() throws InterruptedException {
        num_reads.incrementAndGet();

        int ri=read_index.get();
        int wi=write_index.get();

        // System.out.printf("** _read(): ri=%d, wi=%d\n", ri, wi);

        if(index(ri+1) == wi)
            return 0;

        int sent_msgs=sendBundledMessages(buf, ri, wi);

        // read_index=index(ri + sent_msgs); // publish read_index to main memory
        advanceReadIndex(ri, wi); // publish read_index into main memory
        // System.out.printf("** advancing read_index from %d to %d (write_index=%d)\n", read_index, tmp, write_index);

        return sent_msgs;
    }


    public int _readMessagesNew() throws InterruptedException {
        int sent_msgs=0;

        for(;;) {
            num_reads.incrementAndGet();

            int ri=read_index.get();
            int wi=write_index.get();

            // System.out.printf("** _read(): ri=%d, wi=%d\n", ri, wi);

            if(index(ri+1) == wi)
                break;

            sent_msgs+=sendBundledMessages(buf, ri, wi);

            // read_index=index(ri + sent_msgs); // publish read_index to main memory
            if(!advanceReadIndex(ri, wi)) // publish read_index into main memory
                break;
        }

        return sent_msgs;
    }

    protected boolean advanceReadIndex(int ri, final int wi) {
        boolean advanced=false;
        for(int i=increment(ri); i != wi; i=increment(i)) {
            if(buf[i] != NULL_MSG)
                break;
            buf[i]=null;
            ri=i;
            advanced=true;
        }
        if(advanced)
            read_index.set(ri);
        return advanced;
    }

    protected void readMessages() throws InterruptedException {
        _readMessages();
        LockSupport.park();
    }



    /** Read and send messages in range [read-index+1 .. write_index-1] */
    public int sendBundledMessages(final Message[] buf, final int read_index, final int write_index) {
        int       max_bundle_size=transport.getMaxBundleSize();
        byte[]    cluster_name=transport.cluster_name.chars();
        int       sent_msgs=0;

        for(int i=increment(read_index); i != write_index; i=increment(i)) {
            Message msg=buf[i];
            if(msg == NULL_MSG)
                continue;
            if(msg == null)
                break;

            Address dest=msg.dest();
            try {
                output.position(0);
                Util.writeMessageListHeader(dest, msg.src(), cluster_name, 1, output, dest == null);

                // remember the position at which the number of messages (an int) was written, so we can later set the
                // correct value (when we know the correct number of messages)
                int size_pos=output.position() - Global.INT_SIZE;
                int num_msgs=marshalMessagesToSameDestination(dest, buf, i, write_index, max_bundle_size);
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
        }
        return sent_msgs;
    }

    public String toString() {
        return String.format("read-index=%d write-index=%d size=%d cap=%d\n",
                             read_index.get(), write_index.get(), getBufferSize(), buf.length);
    }



    // Iterate through the following messages and find messages to the same destination (dest) and write them to output
    protected int marshalMessagesToSameDestination(Address dest, Message[] buf, final int start_index, final int end_index,
                                                   int max_bundle_size) throws Exception {
        int num_msgs=0, bytes=0;
        for(int i=start_index; i != end_index; i=increment(i)) {
            Message msg=buf[i];
            if(msg != null && msg != NULL_MSG && Objects.equals(dest, msg.dest())) {
                long msg_size=msg.size();
                if(bytes + msg_size > max_bundle_size)
                    break;
                bytes+=msg_size;
                num_msgs++;
                buf[i]=NULL_MSG;
                msg.writeToNoAddrs(msg.src(), output, transport.getId());
            }
        }
        return num_msgs;
    }

    protected final int increment(int index) {return index+1 == buf.length? 0 : index+1;}
    protected final int index(int idx)     {return idx & (buf.length-1);}    // fast equivalent to %


    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }

    protected class BundlerThread implements Runnable {
        protected volatile boolean running=true;
        protected Thread           runner;


        protected synchronized void start() {
            stop();
            running=true;
            runner=transport.getThreadFactory().newThread(this, THREAD_NAME);
            running=true;
            runner.start();
        }

        protected synchronized void stop() {
            stop(true);
        }

        protected synchronized void stop(boolean clear_queue) {
            running=false;
            _stop(clear_queue);
        }

        protected void _stop(boolean clear_queue) {
            running=false;
            Thread tmp=runner;
            runner=null;
            if(tmp != null) {
                tmp.interrupt();
                if(tmp.isAlive()) {
                    try {tmp.join(500);} catch(InterruptedException e) {}
                }
            }
            if(clear_queue)
                reset();
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

        protected void unpark() {
            LockSupport.unpark(runner);
        }

    }
}
