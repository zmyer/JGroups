package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.auth.AuthToken;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;


/**
 * The AUTH protocol adds a layer of authentication to JGroups. It intercepts join and merge requests and rejects them
 * if the joiner or merger is not permitted to join a or merge into a cluster. AUTH should be placed right below
 * {@link GMS} in the configuration.
 * @author Chris Mills
 * @author Bela Ban
 */
public class AUTH extends Protocol {

    static final String NAME = "AUTH";

    /** Interface to provide callbacks for handling up events */
    public interface UpHandler {
        /**
         * Called when an up event has been received
         * @param evt the event
         * @return true if the event should be passed up, else false
         */
        boolean handleUpEvent(Event evt);
    }


    /** Used on the coordinator to authentication joining member requests against */
    protected AuthToken             auth_token;

    /** List of UpHandler which are called when an up event has been received. Usually used by AuthToken impls */
    protected final List<UpHandler> up_handlers=new ArrayList<UpHandler>();

    protected Address               local_addr;


    public AUTH() {}

    protected volatile boolean      authenticate_coord=true;
    
    // Do join or merge responses from the coordinator also need to be authenticated
    public void setAuthCoord( boolean authenticateCoord) {
        this.authenticate_coord= authenticateCoord;
    }

    public String    getAuthClass()                {return auth_token != null? auth_token.getClass().getName() : null;}
    public AuthToken getAuthToken()                {return auth_token;}
    public void      setAuthToken(AuthToken token) {this.auth_token=token;}
    public void      register(UpHandler handler)   {up_handlers.add(handler);}
    public void      unregister(UpHandler handler) {up_handlers.remove(handler);}
    public Address   getAddress()                  {return local_addr;}


    public boolean setProperties(Properties props) {
        String str;

        str = props.getProperty("auth_class");
        if(str != null){
            props.remove("auth_class");

            try{
                Object obj = Class.forName(str).newInstance();
                auth_token = (AuthToken) obj;
                auth_token.setAuth(this);
                auth_token.setValue(props);
            }catch(Exception e){
                if(log.isFatalEnabled()){
                    log.fatal("Failed to create server side token (" + str + ")");
                    log.fatal(e);
                }
                return false;
            }
        }

        str=props.getProperty("authenticate_coord");
        if(str != null)
        {
            authenticate_coord=Boolean.valueOf(str).booleanValue();
            props.remove("authenticate_coord");
        }

        if(!props.isEmpty()) {
            //this should never happen as everything is read in to the AuthToken instance
            if(log.isErrorEnabled()){
                log.error("AUTH.setProperties(): the following properties are not recognized: " + props);
            }
            return false;
        }
        return true;
    }

    public final String getName() {
        return AUTH.NAME;
    }

    public void init() throws Exception {
        super.init();
        if(auth_token == null)
            throw new IllegalStateException("no authentication mechanism configured");
        auth_token.init();
    }

    public void start() throws Exception {
        super.start();
        if(auth_token != null)
            auth_token.start();
    }

    public void stop() {
        if(auth_token != null)
            auth_token.stop();
        super.stop();
    }

    public void destroy() {
        if(auth_token != null)
            auth_token.destroy();
        super.destroy();
    }

    /**
     * An event was received from the layer below. Usually the current layer will want to examine the event type and
     * - depending on its type - perform some computation (e.g. removing headers from a MSG event type, or updating
     * the internal membership list when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down the stack using {@code down_prot.down()}
     * or c) the event (or another event) is sent up the stack using {@code up_prot.up()}.
     */
    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();

                // If we have a join or merge request --> authenticate, else pass up
                GMS.GmsHeader gms_hdr=getGMSHeader(evt);
                if(gms_hdr != null && needsAuthentication(gms_hdr)) {
                    AuthHeader auth_hdr=(AuthHeader)msg.getHeader(AUTH.NAME);
                    if(auth_hdr == null)
                        throw new IllegalStateException(String.format("found %s from %s but no AUTH header", gms_hdr, msg.getSrc()));
                    if(!handleAuthHeader(gms_hdr, auth_hdr, msg)) // authentication failed
                        return null;    // don't pass up
                }
                break;
        }
        if(!callUpHandlers(evt))
            return null;

        return up_prot.up(evt);
    }



    /**
     * An event is to be sent down the stack. The layer may want to examine its type and perform
     * some action on it, depending on the event's type. If the event is a message MSG, then
     * the layer may need to add a header to it (or do nothing at all) before sending it down
     * the stack using {@code down_prot.down()}. In case of a GET_ADDRESS event (which tries to
     * retrieve the stack's address from one of the bottom layers), the layer may need to send
     * a new response event back up the stack using {@code up_prot.up()}.
     */
    public Object down(Event evt) {
        GMS.GmsHeader hdr = getGMSHeader(evt);
        if(hdr != null && needsAuthentication(hdr)) {
            // we found a join request message - now add an AUTH Header
            Message msg=(Message)evt.getArg();
            msg.putHeader(AUTH.NAME, new AuthHeader(this.auth_token));
        }

        if(evt.getType() == Event.SET_LOCAL_ADDRESS)
            local_addr=(Address)evt.getArg();

        return down_prot.down(evt);
    }



    protected boolean needsAuthentication(GMS.GmsHeader hdr) {
        switch(hdr.getType()) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
            case GMS.GmsHeader.MERGE_REQ:
                return true;
            case GMS.GmsHeader.JOIN_RSP:
            case GMS.GmsHeader.MERGE_RSP:
            case GMS.GmsHeader.INSTALL_MERGE_VIEW:
                return this.authenticate_coord;
            default:
                return false;
        }
    }


    /**
     * Handles a GMS header
     * @param gms_hdr
     * @param msg
     * @return true if the message should be passed up, or else false
     */
    protected boolean handleAuthHeader(GMS.GmsHeader gms_hdr, AuthHeader auth_hdr, Message msg) {
        if(needsAuthentication(gms_hdr)) {
            if(this.auth_token.authenticate(auth_hdr.getToken(), msg))
                return true; //  authentication passed, send message up the stack
            else {
                log.warn(String.format("%s: failed to validate AuthHeader (token: %s) from %s; dropping message",
                         local_addr, auth_token.getClass().getSimpleName(), msg.getSrc()));
                sendRejectionMessage(gms_hdr.getType(), msg.getSrc(), "authentication failed");
                return false;
            }
        }
        return true;
    }


    protected void sendRejectionMessage(byte type, Address dest, String error_msg) {
        switch(type) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                sendJoinRejectionMessage(dest, error_msg);
                break;
            case GMS.GmsHeader.MERGE_REQ:
                sendMergeRejectionMessage(dest);
                break;
        }
    }

    protected void sendJoinRejectionMessage(Address dest, String error_msg) {
        if(dest == null)
            return;

        JoinRsp joinRes=new JoinRsp(error_msg); // specify the error message on the JoinRsp
        Message msg = new Message(dest);
        msg.putHeader(GMS.name, new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP, joinRes));
        if(this.authenticate_coord)
            msg.putHeader(AUTH.NAME, new AuthHeader(this.auth_token));
        down_prot.down(new Event(Event.MSG, msg));
    }

    protected void sendMergeRejectionMessage(Address dest) {
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP);
        hdr.setMergeRejected(true);
        Message msg=new Message(dest);
        msg.setFlag(Message.OOB);
        msg.putHeader(GMS.name, hdr);
        if(this.authenticate_coord)
            msg.putHeader(AUTH.NAME, new AuthHeader(this.auth_token));
        log.debug("merge response=" + hdr);
        down_prot.down(new Event(Event.MSG, msg));
    }

    protected boolean callUpHandlers(Event evt) {
        boolean pass_up=true;
        for(UpHandler handler: up_handlers) {
            if(!handler.handleUpEvent(evt))
                pass_up=false;
        }
        return pass_up;
    }

    /**
     * Get the header from a GMS message
     * @param evt The event object passed in to AUTH
     * @return A GmsHeader object or null if the event contains a message of a different type
     */
    protected static GMS.GmsHeader getGMSHeader(Event evt){
        return evt.getType() == Event.MSG? getGMSHeader((Message)evt.getArg()) : null;
    }

    protected static GMS.GmsHeader getGMSHeader(Message msg){
        Header hdr = msg.getHeader(GMS.name);
        if(hdr instanceof GMS.GmsHeader)
            return (GMS.GmsHeader)hdr;
        return null;
    }
}
