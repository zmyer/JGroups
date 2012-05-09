package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Multicast Flow Control
 * @author Bela Ban
 * @since 3.1
 */
@Experimental
@MBean(description="Multicast Flow Control protocol, based on credits")
public class MFC2 extends Protocol {

    /**
     * Max number of bytes to send per receiver until an ack must be received before continuing sending
     */
    @Property(description="Max number of bytes to send until more credits must be received to proceed")
    protected long max_credits=500000;

    /**
     * Max time (in milliseconds) to block. If credit hasn't been received after max_block_time, we send
     * a REPLENISHMENT request to the members from which we expect credits. A value <= 0 means to wait forever.
     */
    @Property(description="Max time (in milliseconds) to block before sending a credit request")
    protected long max_block_time=5000;



    /**
     * If we're down to (min_threshold * max_credits) bytes for P, we send more credits to P. Example: if
     * max_credits is 1'000'000, and min_threshold 0.25, then we send ca. 250'000 credits to P once we've got only
     * 250'000 credits left for P (we've received 750'000 bytes from P).
     */
    @Property(description="The threshold (as a percentage of max_credits) at which a receiver sends more credits to " +
      "a sender. Example: if max_credits is 1'000'000, and min_threshold 0.25, then we send ca. 250'000 credits " +
      "to P once we've got only 250'000 credits left for P (we've received 750'000 bytes from P)")
    protected double min_threshold=0.40;


    /**
     * Computed as <tt>max_credits</tt> times <tt>min_theshold</tt>. If explicitly set, this will
     * override the above computation
     */
    @Property(description="Computed as max_credits x min_theshold unless explicitly set")
    protected long min_credits=0;
    

    protected final Lock      lock=new ReentrantLock();
    protected final Condition credits_available=lock.newCondition();

    protected long credits;

    /** Whether FlowControl is still running, this is set to false when the protocol terminates (on stop()) */
    protected volatile boolean running=true;

    @ManagedAttribute(description="Number of credits to send when falling under min_threshold bytes")
    protected long credits_to_replenish=max_credits;

     /** Last time a credit request was sent. Used to prevent credit request storms */
    protected long last_credit_request=0;

    protected Address local_addr;


    
    public MFC2() {
    }

    public void resetStats() {
        super.resetStats();
    }

    public long getMaxCredits() {
        return max_credits;
    }

    public void setMaxCredits(long max_credits) {
        this.max_credits=max_credits;
    }

    public double getMinThreshold() {
        return min_threshold;
    }

    public void setMinThreshold(double min_threshold) {
        this.min_threshold=min_threshold;
    }

    public long getMinCredits() {
        return min_credits;
    }

    public void setMinCredits(long min_credits) {
        this.min_credits=min_credits;
    }


    public void init() throws Exception {
        boolean min_credits_set=min_credits != 0;
        if(!min_credits_set)
            min_credits=(long)(max_credits * min_threshold);
        credits=max_credits;
    }

    public void start() throws Exception {
        super.start();
        running=true;
    }

    public void stop() {
        super.stop();
        running=false;
    }



    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.NO_FC) || msg.getDest() != null)
                    break;

                int length=msg.getLength();
                if(length == 0)
                    break;

                return handleDownMessage(evt, length);

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt); // this could potentially use the lower protocol's thread which may block
    }



    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.NO_FC) || msg.getDest() != null)
                    break;

                FcHeader hdr=(FcHeader)msg.getHeader(this.id);

                if(hdr != null) {
                    /*switch(hdr.type) {
                        case FcHeader.REPLENISH:
                            num_credit_responses_received++;
                            handleCredit(msg.getSrc(), (Long)msg.getObject());
                            break;
                        case FcHeader.CREDIT_REQUEST:
                            num_credit_requests_received++;
                            Address sender=msg.getSrc();
                            Long requested_credits=(Long)msg.getObject();
                            if(requested_credits != null)
                                handleCreditRequest(received, sender, requested_credits.longValue());
                            break;
                        default:
                            log.error("header type " + hdr.type + " not known");
                            break;
                    }*/
                    return null; // don't pass message up
                }

                Address sender=msg.getSrc();

                long length=msg.getLength();
                if(length == 0)
                    break;
                boolean send_replenishment=false;

                lock.lock();
                try {
                    if(!local_addr.equals(sender)) {
                        credits=Math.max(0, credits-length);
                    }
                    if(credits <= min_credits) {
                        send_replenishment=true;
                    }
                }
                finally {
                    lock.unlock();
                }

                try {
                    return up_prot.up(evt);
                }
                finally {
                    if(send_replenishment)
                        sendReplenishment();
                }

            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()));
                break;
        }
        return up_prot.up(evt);
    }


    protected Object handleDownMessage(final Event evt, int length) {
        while(running) {

            lock.lock();
            try {
                if(credits - length >= 0) {
                    credits-=length;
                    break;
                }
                if(credits > 0) {
                    length-=credits;
                    credits=0;
                }
                try {
                    boolean rc=credits_available.await(max_block_time,TimeUnit.MILLISECONDS);
                    if(!rc && needToSendCreditRequest())
                        sendCreditRequest();
                }
                catch(InterruptedException e) {
                }
            }
            finally {
                lock.unlock();
            }
        }

        // send message - either after regular processing, or after blocking (when enough credits are available again)
        return down_prot.down(evt);
    }

    protected void handleViewChange(View view) {
        credits_to_replenish=max_credits / view.size();
    }

    protected synchronized boolean needToSendCreditRequest() {
        long curr_time=System.currentTimeMillis();
        long wait_time=curr_time - last_credit_request;
        if(wait_time >= max_block_time) {
            last_credit_request=curr_time;
            return true;
        }
        return false;
    }


    protected void sendCreditRequest() {
        // todo
    }

    protected void sendReplenishment() {
        // todo
    }
}
