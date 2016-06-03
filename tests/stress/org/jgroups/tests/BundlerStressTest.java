package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.protocols.*;
import org.jgroups.util.AsciiString;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.jgroups.tests.FragTest2.type;

/**
 * Tests bundler performance
 * @author Bela Ban
 * @since  4.0
 */
public class BundlerStressTest {
    protected String                 bundler_type;
    protected Bundler                bundler;
    protected int                    num_msgs=20000, num_senders=20;
    protected static final Address[] ADDRESSES;
    protected final TP               transport=new MockTransport();


    static {
        ADDRESSES=new Address[]{null, Util.createRandomAddress("A"), Util.createRandomAddress("B"),
          Util.createRandomAddress("C"), Util.createRandomAddress("D"), Util.createRandomAddress("E"),
          Util.createRandomAddress("F"), Util.createRandomAddress("G"), Util.createRandomAddress("H")};
    }


    public BundlerStressTest(String bundler_type) {
        this.bundler_type=bundler_type;
    }

    protected void start() {
        this.bundler=createBundler(bundler_type);
        this.bundler.init(transport);
        this.bundler.start();
        loop();
    }

    protected void loop() {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("[1] send [2] num_msgs (%d) [3] senders (%d)\n" +
                                                "[b] change bundler (%s) [x] exit\n",
                                              num_msgs, num_senders, bundler.getClass().getSimpleName()));
            try {
                switch(c) {
                    case '1':
                        sendMessages();
                        break;
                    case '2':
                        num_msgs=Util.readIntFromStdin("num_msgs: ");
                        break;
                    case '3':
                        num_senders=Util.readIntFromStdin("num_senders: ");
                        break;
                    case 'b':
                        try {
                            String type=Util.readStringFromStdin("new bundler type: ");
                            Bundler old=this.bundler;
                            this.bundler=createBundler(type);
                            this.bundler.init(transport);
                            this.bundler.start();
                            if(old != null)
                                old.stop();
                        }
                        catch(Throwable t) {
                            System.err.printf("failed changing bundler to %s: %s\n", type, t);
                        }
                        break;
                    case 'x':
                        looping=false;
                        break;
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
        if(this.bundler != null)
            this.bundler.stop();
    }

    protected void sendMessages() throws Exception {
        Message[] msgs=generateMessages(num_msgs);
        CountDownLatch latch=new CountDownLatch(num_senders+1);
        AtomicInteger index=new AtomicInteger(0);
        Sender[] senders=new Sender[num_senders];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(latch, msgs, index);
            senders[i].start();
        }

        long start=Util.micros();
        latch.countDown(); // starts all sender threads

        for(Sender sender: senders)
            sender.join();
        long time_us=Util.micros()-start;
        AverageMinMax send_avg=null;
        for(Sender sender: senders) {
            System.out.printf("[%d] count=%d, send-time = %s\n", sender.getId(), sender.send.count(), sender.send);
            if(send_avg == null)
                send_avg=sender.send;
            else
                send_avg.merge(sender.send);
        }

        double msgs_sec=num_msgs / (time_us / 1_000.0);

        System.out.printf(Util.bold("\n\nreqs/sec    = %.2f" +
                                      "\nsend-time  = min/avg/max: %d / %.2f / %d ns\n"),
                          msgs_sec, send_avg.min(), send_avg.average(), send_avg.max());
    }

    protected Bundler createBundler(String bundler) {
        if(bundler == null)
            throw new IllegalArgumentException("bundler type has to be non-null");
        if(bundler.equals("stq"))
            return new SimplifiedTransferQueueBundler(20000);
        if(bundler.equals("tq"))
            return new TransferQueueBundler(20000);
        if(bundler.startsWith("sender-sends") || bundler.equals("ss"))
            return new SenderSendsBundler();
        if(bundler.startsWith("no-bundler") || bundler.equals("nb"))
            return new NoBundler().poolSize(16348).initialBufSize(25);
        try {
            Class<Bundler> clazz=Util.loadClass(bundler, getClass());
            return clazz.newInstance();
        }
        catch(Throwable t) {
            throw new IllegalArgumentException(String.format("failed creating instance of bundler %s: %s", bundler, t));
        }
    }

    protected static Message[] generateMessages(int num) {
        Message[] msgs=new Message[num];
        for(int i=0; i < msgs.length; i++)
            msgs[i]=new Message(pickAddress());
        return msgs;
    }

    protected static Address pickAddress() {
        return Util.pickRandomElement(ADDRESSES);
    }

    public static void main(String[] args) {
        String bundler="no-bundler";
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-bundler")) {
                bundler=args[++i];
                continue;
            }
            System.out.printf("BundlerStressTest [-bundler bundler-type]\n");
            return;
        }
        new BundlerStressTest(bundler).start();
    }


    protected class Sender extends Thread {
        protected final CountDownLatch latch;
        protected final Message[]      msgs;
        protected final AtomicInteger  index;
        protected final AverageMinMax  send=new AverageMinMax(); // ns

        public Sender(CountDownLatch latch, Message[] msgs, AtomicInteger index) {
            this.latch=latch;
            this.msgs=msgs;
            this.index=index;
        }

        public void run() {
            latch.countDown();
            while(true) {
                int idx=index.getAndIncrement();
                if(idx >= msgs.length)
                    break;
                try {
                    long start=System.nanoTime();
                    bundler.send(msgs[idx]);
                    long time_ns=System.nanoTime()-start;
                    send.add(time_ns);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected static class MockTransport extends TP {

        public MockTransport() {
            this.cluster_name=new AsciiString("mock");
        }

        public boolean supportsMulticasting() {
            return false;
        }

        public void doSend(byte[] buf, int offset, int length, Address dest) throws Exception {

        }

        public void sendMulticast(byte[] data, int offset, int length) throws Exception {

        }

        public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {

        }

        public String getInfo() {
            return null;
        }

        protected PhysicalAddress getPhysicalAddress() {
            return null;
        }
    }

}
