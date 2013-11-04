package org.jgroups.tests;

import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.protocols.TP;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.Util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Bela Ban
 * @since  3.5
 */
public class BundlerStressTest {
    protected TP.Bundler bundler;
    protected int        num_msgs=1000000;
    protected int        num_senders=25;
    protected boolean    new_bundler=true;
    protected int        bundler_capacity=20000;

    public BundlerStressTest(boolean new_bundler, int num_msgs, int num_senders, int bundler_capacity) {
        this.num_msgs=num_msgs;
        this.num_senders=num_senders;
        this.new_bundler=new_bundler;
        this.bundler_capacity=bundler_capacity;
    }

    protected void start() throws Exception {
        MyTransport transport=new MyTransport().channelName("cluster");
        transport.setTimer(new TimeScheduler3());
        // transport.setValue("bundler_capacity", bundler_capacity);


        bundler=new_bundler? transport.createTransferQueueBundler(bundler_capacity) : transport.createDefaultBundler();
        System.out.println("bundler is " + bundler.getClass().getSimpleName() + ", capacity=" + bundler_capacity);
        bundler.start();

        final AtomicInteger count=new AtomicInteger(0);
        final CountDownLatch latch=new CountDownLatch(num_senders+1);
        Sender[] senders=new Sender[num_senders];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(num_msgs, count, bundler, latch);
            senders[i].start();
        }
        long start=System.currentTimeMillis();
        latch.countDown(); // start all senders

        for(Sender sender: senders)
            sender.join();

        if(bundler instanceof TP.TransferQueueBundler) {
            while(((TP.TransferQueueBundler)bundler).getBufferSize() > 0)
                Util.sleep(10);
        }
        else if(bundler instanceof TP.DefaultBundler || bundler instanceof TP.DefaultBundler2) {
            ; // joining the senders did the job
        }
        else
            throw new IllegalStateException("bundler " + bundler.getClass().getSimpleName() + " not known");

        long time=System.currentTimeMillis() - start;
        double msgs_sec=num_msgs / (time / 1000.0);
        System.out.println("time=" + time + " ms, " + String.format("%.2f", msgs_sec) + " msgs/sec");

        bundler.stop();
        transport.stop();
    }


    public static void main(String[] args) throws Exception {
        int     num_msgs=1000000;
        int     num_senders=25;
        int     bundler_capacity=20000;
        boolean new_bundler=true;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num_senders")) {
                num_senders=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_msgs")) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-bundler")) {
                String bundler_type=args[++i];
                if(bundler_type.startsWith("old"))
                    new_bundler=false;
                continue;
            }
            if(args[i].equals("-bundler_capacity")) {
                bundler_capacity=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("BundlerStressTest [-num_msgs msgs] [-num_senders sender] [-bundler old|new] [-bundler_capacity capacity]");
            return;
        }
        new BundlerStressTest(new_bundler, num_msgs, num_senders, bundler_capacity).start();
    }


    protected static class Sender extends Thread {
        protected final int            max_msgs;
        protected final AtomicInteger  count;
        protected final TP.Bundler     bundler;
        protected final CountDownLatch latch;
        protected final Message        msg=new Message(null, new byte[1000]);
        protected final int            print;

        public Sender(int max_msgs, AtomicInteger count, TP.Bundler bundler, CountDownLatch latch) {
            this.max_msgs=max_msgs;
            this.bundler=bundler;
            this.latch=latch;
            this.count=count;
            print=max_msgs / 10;
        }

        public void run() {
            latch.countDown();
            while(true) {
                int num=count.incrementAndGet();
                if(num > max_msgs)
                    break;
                try {
                    bundler.send(msg);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }

                if(num > 0 && num % print == 0) {
                    if(bundler instanceof TP.TransferQueueBundler)
                        System.out.println(num + ", size=" + ((TP.TransferQueueBundler)bundler).getBufferSize());
                    else
                        System.out.println(num);
                }
            }
        }
    }


    protected static class MyTransport extends TP {
         // protected int                 msgs_to_send;
        // protected final AtomicInteger count=new AtomicInteger(0);

        public boolean supportsMulticasting() {return true;}
        public void sendMulticast(byte[] data, int offset, int length) throws Exception {}
        public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
        public String getInfo() {return null;}
        protected PhysicalAddress getPhysicalAddress() {return null;}

        public ThreadFactory getThreadFactory() {
            return new DefaultThreadFactory("bla", true);
        }

        protected MyTransport channelName(String name) {
            this.channel_name=name; return this;
        }

        public void stop() {
            super.stop();
            timer.stop();
        }

        protected Bundler createTransferQueueBundler(int capacity) {
            return new TransferQueueBundler(capacity);
        }

        protected Bundler createDefaultBundler() {
            return new DefaultBundler2();
        }

        protected Bundler createDefaultBundler2() {
            return new DefaultBundler2();
        }
    }

}
