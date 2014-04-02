
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Implementation of total order protocol using a sequencer_uum.
 * 
 * INSERT DESCRIPTION HERE
 * 
 * @author Bela Ban
 * @edited Andrei Palade
 */
@MBean(description="Implementation of total order protocol using a sequencer (unicast-unicast-multicast)")
public class SEQUENCER_UUM extends Protocol {
    protected Address                           local_addr;
    protected volatile Address                  coord;
    protected volatile View                     view;
    protected volatile boolean                  is_coord=false;
    protected final AtomicLong                  seqno=new AtomicLong(0);
    
    /** Maintains requests made to the coord for which no ACK has been received yet.
     *  Needs to be sorted so we resend them in the right order */
  //  protected final NavigableMap<Long,Message>  request_table=new ConcurrentSkipListMap<Long,Message>();  
    protected final NavigableMap<Long,Message>  request_table=new ConcurrentSkipListMap<Long,Message>();  

    /** Maintains responses made to the requests from senders; When the coord receives a broadcast from sending processes
     *  it acknowledges the successful retrieval of the response and removes the message from this map; We do this so we
     *  avoid an ACK message from the sender to coordinator. We say that the ACK has been piggybacked on the message during multicast
     *  Needs to be sorted so we resend them in the right order */
    protected final NavigableMap<Long,Long>  response_table=new ConcurrentSkipListMap<Long,Long>();
    
    protected final Lock                        send_lock=new ReentrantLock();

    protected final Condition                   send_cond=send_lock.newCondition();

    /** When ack_mode is set, we need to wait for an ack for each forwarded message until we can send the next one */
    protected volatile boolean                  ack_mode=true;

    /** Set when we block all sending threads to resend all messages from forward_table */
    protected volatile boolean                  flushing=false;

    protected volatile boolean                  running=true;

    /** Keeps track of the threads sending messages */
    protected final AtomicInteger               in_flight_sends=new AtomicInteger(0);

    protected volatile Flusher                  flusher;

    /** Used for each resent message to wait until the message has been received */
    protected final Promise<Long>               ack_promise=new Promise<Long>();

    @Property(description="Size of the set to store received seqnos (for duplicate checking)")
    protected int  delivery_table_max_size=2000;

    @Property(description="Number of acks needed before going from ack-mode to normal mode. " +
      "0 disables this, which means that ack-mode is always on")
    protected int  threshold=10;

    protected int  num_acks=0;

    protected long request_msgs=0;
    protected long response_msgs=0;
    protected long bcast_msgs=0;
    
    protected long received_bcasts=0;
    protected long delivered_bcasts=0;
    protected long broadcasts_sent=0;
    
    protected long sent_requests=0;    
    protected long received_requests=0;
    protected long sent_responses=0;
    protected long received_responses=0;

    protected Table<Message>  received_msgs = new Table<Message>();
    private int max_msg_batch_size = 100;

    @ManagedAttribute
    public boolean isCoordinator() {return is_coord;}
    public Address getCoordinator() {return coord;}
    public Address getLocalAddress() {return local_addr;}
    @ManagedAttribute
    public long getBroadcast() {return bcast_msgs;}
    @ManagedAttribute
    public long getReceivedRequests() {return received_requests;}
    @ManagedAttribute
    public long getReceivedBroadcasts() {return received_bcasts;}  
    
    @ManagedAttribute(description="Number of messages in the request-table")
    public int getRequestTableSize() {return request_table.size();}
    
    @ManagedAttribute(description="Number of messages in the response-table")
    public int getResponseTableSize() {return response_table.size();}

    public void setThreshold(int new_threshold) {this.threshold=new_threshold;}

    public void setDeliveryTableMaxSize(int size) {delivery_table_max_size=size;}
    
    @ManagedOperation
    public void resetStats() {
        request_msgs=response_msgs=bcast_msgs=received_bcasts=delivered_bcasts=broadcasts_sent=0L;
        sent_requests=received_requests=sent_responses=received_responses=0L; // reset number of sent and received requests and responses
    }

    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> m=super.dumpStats();
        m.put("requests", request_msgs);
        m.put("responses", response_msgs);
        m.put("broadcast",bcast_msgs);
        
        m.put("sent_requests", sent_requests);
        m.put("received_requests", received_requests);
        m.put("sent_responses", sent_responses);
        m.put("received_responses", received_responses);
        
        m.put("received_bcasts",   received_bcasts);
        m.put("delivered_bcasts",  delivered_bcasts);
        m.put("broadcasts_sent",   broadcasts_sent);
        return m;
    }

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }


	public void start() throws Exception {
		super.start();
		running = true;
		ack_mode = true;
	}

    public void stop() {
        running=false;
        unblockAll();
        stopFlusher();
        super.stop();
    }

    public long getBroadcastsSent(){
    	return broadcasts_sent;
    }
    
    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
            	
                Message msg=(Message)evt.getArg();
                if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;
                
                if(log.isTraceEnabled())
                	log.trace("[" + local_addr + "]: is coordinator:" + (is_coord ? "Yes" : "No") + " in 'down' " + msg.printHeaders());
                
                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);

                if(flushing)
                    block();

                // A seqno is not used to establish ordering, but only to weed out duplicates; next_seqno doesn't need
                // to increase monotonically, but only to be unique (https://issues.jboss.org/browse/JGRP-1461) !
                long next_seqno=seqno.incrementAndGet();
                in_flight_sends.incrementAndGet();
                try {
                    if(log.isTraceEnabled())
                        log.trace("[" + local_addr + "]: requesting from " + local_addr + "::" + seqno + " to coord " + coord);

                    // We always forward messages to the coordinator, even if we're the coordinator. Having the coord
                    // send its messages directly led to starvation of messages from other members. MPerf perf went up
                    // from 20MB/sec/node to 50MB/sec/node with this change !
                    forwardToCoord(next_seqno, msg);
                } catch(Exception ex) {
                    log.error("failed sending message", ex);
                } finally {
                    in_flight_sends.decrementAndGet();
                }
                return null; // don't pass down

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.TMP_VIEW:
                handleTmpView((View)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        Message msg;
        SequencerHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;
                hdr=(SequencerHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break; // pass up
                                
                switch(hdr.type) {
                	case SequencerHeader.FLUSH:
                    case SequencerHeader.REQUEST:
                    	if(!is_coord) {
                            if(log.isErrorEnabled())
                                log.error(local_addr + ": non-coord; dropping REQUEST request from " + msg.getSrc());
                            return null;
                        }
                        Address sender=msg.getSrc();
                        if(view != null && !view.containsMember(sender)) {
                            if(log.isErrorEnabled())
                                log.error(local_addr + ": dropping REQUEST from non-member " + sender + "; view=" + view);
                            return null;
                        }
                        
                        long new_seqno=sender != coord ? seqno.incrementAndGet() : hdr.seqno;
                        
                        unicast(sender,new_seqno,hdr.seqno, hdr.type==SequencerHeader.FLUSH);
                        
                        received_requests++;
                        break;
                        
                	case SequencerHeader.RESPONSE:
                		Address coordinator=msg.getSrc();
                        if(view != null && !view.containsMember(coordinator)) {
                            if(log.isErrorEnabled())
                                log.error(local_addr + ": dropping RESPONSE from non-coordinator " + coordinator + "; view=" + view);
                            return null;
                        }
                        
                        
                        response_table.put(hdr.localSeqno, hdr.seqno);	
                        Message bcast_msg=request_table.get(hdr.localSeqno);

                        System.out.println("--> " + local_addr + "::" + hdr.seqno);
                        broadcast(bcast_msg, hdr.seqno, hdr.localSeqno, hdr.flush_ack); // do copy the message
	                    break;
                    
                    case SequencerHeader.BCAST:
                        deliver(msg, evt, hdr, hdr.flush_ack);
                        received_bcasts++;
                        break;

                    case SequencerHeader.WRAPPED_BCAST:
                        unwrapAndDeliver(msg, hdr.flush_ack);  // unwrap the original message (in the payload) and deliver it
                        received_bcasts++;
                        break;
                }
                return null;

            case Event.VIEW_CHANGE:
                Object retval=up_prot.up(evt);
                handleViewChange((View)evt.getArg());
                return retval;

            case Event.TMP_VIEW:
                handleTmpView((View)evt.getArg());
                break;
        }

        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
                continue;

            // simplistic implementation
            try {
                batch.remove(msg);
                up(new Event(Event.MSG, msg));
            } catch(Throwable t) {
                log.error("failed passing up message", t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /* --------------------------------- Private Methods ----------------------------------- */

    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        if(view == null || view.compareTo(v) < 0)
            view=v;
        else
            return;

        Address existing_coord=coord, new_coord=mbrs.get(0);
        boolean coord_changed=existing_coord == null || !existing_coord.equals(new_coord);
        if(coord_changed && new_coord != null) {
            stopFlusher();
            startFlusher(new_coord); // needs to be done in the background, to prevent blocking if down() would block
        }
    }

    protected void flush(final Address new_coord) throws InterruptedException {
        // wait until all threads currently sending messages have returned (new threads after flushing=true) will block
        // flushing is set to true in startFlusher()
        while(flushing && running) {
            if(in_flight_sends.get() == 0)
                break;
            Thread.sleep(100);
        }

        send_lock.lockInterruptibly();
        try {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": coord changed from " + coord + " to " + new_coord);
            coord=new_coord;
            is_coord=local_addr != null && local_addr.equals(coord);
            flushMessagesInForwardTable();
        }
        finally {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing completed");
            flushing=false;
            ack_mode=true; // go to ack-mode after flushing
            num_acks=0;
            send_cond.signalAll();
            send_lock.unlock();
        }
    }

    // If we're becoming coordinator, we need to handle TMP_VIEW as
    // an immediate change of view. See JGRP-1452.
    private void handleTmpView(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        Address new_coord=mbrs.get(0);
        if(!new_coord.equals(coord) && local_addr != null && local_addr.equals(new_coord))
            handleViewChange(v);
    }

    /**
     * Sends all messages currently in forward_table to the new coordinator (changing the dest field).
     * This needs to be done, so the underlying reliable unicast protocol (e.g. UNICAST) adds these messages
     * to its retransmission mechanism<br/>
     * Note that we need to resend the messages in order of their seqnos ! We also need to prevent other message
     * from being inserted until we're done, that's why there's synchronization.<br/>
     * Access to the forward_table doesn't need to be synchronized as there won't be any insertions during flushing
     * (all down-threads are blocked)
     */
    protected void flushMessagesInForwardTable() {
        if(is_coord) {
            for(Map.Entry<Long,Message> entry: request_table.entrySet()) {
            	if(!response_table.containsKey(entry.getKey())){
                    SequencerHeader hdr=new SequencerHeader(SequencerHeader.REQUEST, entry.getKey());
                    Message forward_msg=new Message(coord).putHeader(this.id, hdr);
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": flushing (unicasting) lost requests " + local_addr + "::" + entry.getKey());
                    down_prot.down(new Event(Event.MSG, forward_msg));
            	} 
            }
            return;
        }

        // for forwarded messages, we need to receive the forwarded message from the coordinator, to prevent this case:
        // - V1={A,B,C}
        // - A crashes
        // - C installs V2={B,C}
        // - C forwards messages 3 and 4 to B (the new coord)
        // - B drops 3 because its view is still V1
        // - B installs V2
        // - B receives message 4 and broadcasts it
        // ==> C's message 4 is delivered *before* message 3 !
        // ==> By resending 3 until it is received, then resending 4 until it is received, we make sure this won't happen
        // (see https://issues.jboss.org/browse/JGRP-1449)
        
        NavigableMap<Long,Message> missing_request_table=new ConcurrentSkipListMap<Long,Message>();
    	for(Map.Entry<Long,Message> entry: request_table.entrySet()) {
        	if(!response_table.containsKey(entry.getKey())){
        		request_table.put(entry.getKey(), entry.getValue());
        	}
    	}
        
        while(flushing && running && !missing_request_table.isEmpty()) {
            Map.Entry<Long,Message> entry=missing_request_table.firstEntry();
            final Long key=entry.getKey();

            while(flushing && running && !missing_request_table.isEmpty()) {
                SequencerHeader hdr=new SequencerHeader(SequencerHeader.FLUSH, key);
                Message forward_msg=new Message(coord).putHeader(this.id,hdr).setFlag(Message.Flag.DONT_BUNDLE);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": flushing (forwarding) " + local_addr + "::" + key + " to coord " + coord);
                ack_promise.reset();
                down_prot.down(new Event(Event.MSG, forward_msg));
                Long ack=ack_promise.getResult(500);
                if((ack != null && ack.equals(key)) || !request_table.containsKey(key))
                    break;
            }
        }
    }
    
    protected void forwardToCoord(long seqno, Message msg) {
    	
        if(is_coord) {
        	request_table.put(seqno, msg);
            forward(seqno, false);
            return;
        }

        if(!running || flushing) {
        	request_table.put(seqno, msg);
            return;
        }

        if(!ack_mode) {
        	request_table.put(seqno, msg);
            forward(seqno, false);
            return;
        }

        send_lock.lock();
        try {
        	request_table.put(seqno, msg);
            while(running && !flushing) {
                ack_promise.reset();
                forward(seqno, true);
                if(!ack_mode || !running || flushing)
                    break;
                Long ack=ack_promise.getResult(500);
                if((ack != null && ack.equals(seqno)) || !request_table.containsKey(seqno))
                    break;
            }
        } finally {
            send_lock.unlock();
        }
    }   

    protected void forward(long seqno, boolean flush) {
        Address target=coord;
        if(target == null)
            return;
        byte type=flush ? SequencerHeader.FLUSH : SequencerHeader.REQUEST;
        
        SequencerHeader hdr=new SequencerHeader(type,seqno);
        Message forward_msg=new Message(target).putHeader(this.id,hdr);
     
        down_prot.down(new Event(Event.MSG, forward_msg));
        sent_requests++;
    }
    
	protected void unicast(Address original_sender, long new_seqno, long old_seqno, boolean resend) {		
		SequencerHeader hdr = new SequencerHeader(SequencerHeader.RESPONSE, new_seqno, old_seqno);
		Message ucast_msg = new Message(original_sender).putHeader(this.id, hdr);
		
		if(resend) {
            hdr.flush_ack=true;
        }

		if (log.isTraceEnabled())
			log.trace(local_addr + ": unicasting " + original_sender + ":: new_seqno=" + new_seqno + " local_seqno=" + old_seqno);

        down_prot.down(new Event(Event.MSG, ucast_msg));
        sent_responses++;
	}
    
    protected void broadcast(final Message msg, long seqno, long local_seqno, boolean resend) {

    	SequencerHeader new_hdr=new SequencerHeader(SequencerHeader.BCAST, seqno, local_seqno);
        msg.putHeader(this.id, new_hdr);
 
        if(resend) {
            new_hdr.flush_ack=true;
            msg.setFlag(Message.Flag.DONT_BUNDLE);
        }

        if(log.isTraceEnabled())
            log.trace(local_addr + ": broadcasting ::" + seqno);

        if(msg.getLength() >= 1000){
        	
        		// if(is_coord){
        	
	        	//	broadcasts_sent++;
	        	
//	                long bsent = getBroadcastsSent();
//					long urec = ((UNICAST3)UNICAST3).getUnicastsReceived();
//					long retransmit = ((UNICAST3)UNICAST3).getRetransmissions();
//					if (start == -1) {
//    					start = System.currentTimeMillis();
//    				}
	
					// writer.println(bsent + ", " + urec + ", " + retransmit + ", " + (System.currentTimeMillis() - start));
        		//}
        }
        
        down_prot.down(new Event(Event.MSG, msg));
        bcast_msgs++;
    }

    /**
     * Unmarshal the original message (in the payload) and then pass it up (unless already delivered)
     * @param msg
     */
    protected void unwrapAndDeliver(final Message msg, boolean flush_ack) {
        try {
            Message msg_to_deliver=(Message)Util.objectFromByteBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
        
            SequencerHeader hdr=(SequencerHeader)msg.getHeader(this.id);
            if(flush_ack)
                hdr.flush_ack=true;
            
            deliver(msg_to_deliver, new Event(Event.MSG, msg_to_deliver), hdr, flush_ack);
        } catch(Exception ex) {
            log.error("failure unmarshalling buffer", ex);
        }
    }

    protected void deliver(Message msg, Event evt, SequencerHeader hdr, boolean flush_ack) {
    	
    	if(flush_ack){
             hdr.flush_ack=true;
    	}
    	
        Address sender=msg.getSrc();
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender is null, cannot deliver " + "::" + hdr.getSeqno());
            return;
        }
        
        final Table<Message> win=received_msgs;

        if(sender.equals(local_addr)) {
            request_table.remove(hdr.localSeqno);
            response_table.remove(hdr.localSeqno);
            if(hdr.flush_ack) {
                ack_promise.setResult(hdr.seqno);
                if(ack_mode && !flushing && threshold > 0 && ++num_acks >= threshold) {
                    ack_mode=false;
                    num_acks=0;
                }
            }
        }
        if(is_coord){
        	response_table.remove(hdr.seqno);
        }
        
//        if(!canDeliver(sender, hdr.seqno)){
//            if(log.isWarnEnabled())
//                log.warn(local_addr + ": dropped duplicate message " + sender + "::" + hdr.seqno);
//            return;
//        }

        System.out.println("<-- " + msg.getSrc() + "::" + hdr.seqno);

        win.add(hdr.seqno, msg);
        
        if(log.isTraceEnabled())
        	log.trace(local_addr + ": delivering " + sender + "::" + hdr.seqno);

        
        final AtomicBoolean processing=win.getProcessing();
        if(processing.compareAndSet(false, true)) 
            removeAndDeliver(processing, win, sender);
        
        
 //       up_prot.up(evt);
 //       delivered_bcasts++;
    }
    
    
    protected void removeAndDeliver(final AtomicBoolean processing, Table<Message> win, Address sender) {
        boolean released_processing=false;
        try {
            while(true) {
                List<Message> list=win.removeMany(processing, true, max_msg_batch_size);
                if(list != null) // list is guaranteed to NOT contain any OOB messages as the drop_oob_msgs_filter removed them
                    deliverBatch(new MessageBatch(local_addr, sender, null, false, list));
                else {
                    released_processing=true;
                    return;
                }
            }
        }
        finally {
            // processing is always set in win.remove(processing) above and never here ! This code is just a
            // 2nd line of defense should there be an exception before win.removeMany(processing) sets processing
            if(!released_processing)
                processing.set(false);
        }
    }
    
    protected void deliverBatch(MessageBatch batch) {
        try {
            if(batch.isEmpty())
                return;
            if(log.isTraceEnabled()) {
                Message first=batch.first(), last=batch.last();
                StringBuilder sb=new StringBuilder(local_addr + ": delivering");
                if(first != null && last != null) {
                    SequencerHeader hdr1=(SequencerHeader)first.getHeader(id), hdr2=(SequencerHeader)last.getHeader(id);
                    sb.append(" #").append(hdr1.seqno).append(" - #").append(hdr2.seqno);
                }
                sb.append(" (" + batch.size()).append(" messages)");
                log.trace(sb);
            }
            up_prot.up(batch);
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "batch", batch, t);
        }
    }


    protected void block() {
        send_lock.lock();
        try {
            while(flushing && running) {
                try {
                    send_cond.await();
                }
                catch(InterruptedException e) {
                }
            }
        }
        finally {
            send_lock.unlock();
        }
    }

    protected void unblockAll() {
        flushing=false;
        send_lock.lock();
        try {
            send_cond.signalAll();
            ack_promise.setResult(null);
        }
        finally {
            send_lock.unlock();
        }
    }

    protected synchronized void startFlusher(final Address new_coord) {
        if(flusher == null || !flusher.isAlive()) {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing started");
            // causes subsequent message sends (broadcasts and forwards) to block (https://issues.jboss.org/browse/JGRP-1495)
            flushing=true;
            
            flusher=new Flusher(new_coord);
            flusher.setName("Flusher");
            flusher.start();
        }
    }

    protected void stopFlusher() {
        flushing=false;
        Thread tmp=flusher;

        while(tmp != null && tmp.isAlive()) {
            tmp.interrupt();
            ack_promise.setResult(null);
            try {
                tmp.join();
            }
            catch(InterruptedException e) {
            }
        }
    }

    /* ----------------------------- End of Private Methods -------------------------------- */
    
	protected class Flusher extends Thread {
		protected final Address new_coord;

		public Flusher(Address new_coord) {
			this.new_coord = new_coord;
		}

		public void run() {
			try {
				flush(new_coord);
			} catch (InterruptedException e) {
			}
		}
	}

    public static class SequencerHeader extends Header {
    	
        protected static final byte REQUEST       = 1;
        protected static final byte FLUSH         = 2;
        protected static final byte BCAST         = 3;
        protected static final byte WRAPPED_BCAST = 4;
        protected static final byte RESPONSE      = 5;

        protected byte    type=-1;
        protected long    seqno=-1;
        protected long    localSeqno=-1;
        protected boolean flush_ack;

        public SequencerHeader() {}

        public SequencerHeader(byte type) {this.type=type;}

        public SequencerHeader(byte type, long seqno) {this(type); this.seqno=seqno;}
        
        public SequencerHeader(byte type, long seqno, long localSeqno) {
        	this(type); 
        	this.seqno=seqno; 
        	this.localSeqno=localSeqno;
        }

        public long getSeqno() {return seqno;}
        
        public long getLocalSeqno() {return localSeqno;}

        public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(seqno >= 0)
                sb.append(" seqno=" + seqno);
            if(localSeqno >= 0)
                sb.append(" localSeqno=" + localSeqno);
            if(flush_ack)
                sb.append(" (flush_ack)");
            return sb.toString();
        }

        protected final String printType() {
            switch(type) {
                case REQUEST:        return "REQUEST";
                case FLUSH:          return "FLUSH";
                case BCAST:          return "BCAST";
                case WRAPPED_BCAST:  return "WRAPPED_BCAST";
                case RESPONSE:	     return "RESPONSE";
                default:             return "n/a";
            }
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            Bits.writeLong(localSeqno,out);
            out.writeBoolean(flush_ack);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Bits.readLong(in);
            localSeqno=Bits.readLong(in);
            flush_ack=in.readBoolean();
        }

        // type + seqno + localSeqno + flush_ack
        public int size() {
            return Global.BYTE_SIZE + Bits.size(seqno) + Bits.size(localSeqno) + Global.BYTE_SIZE; 
        }
    }

}