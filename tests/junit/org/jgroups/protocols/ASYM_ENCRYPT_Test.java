package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.auth.MD5Token;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.Vector;
import java.util.Properties;

/**
 * Tests use cases for {@link ASYM_ENCRYPT} described in https://issues.jboss.org/browse/JGRP-2021.
 * @author Bela Ban
 * @since  4.0
 */
public class ASYM_ENCRYPT_Test extends EncryptTest {
    protected String getProtocolName()
    {
        return "ASYM_ENCRYPT";
    }

    /**
     * A non-member sends a {@link EncryptHeader#SECRET_KEY_REQ} request to the key server. Asserts that the rogue member
     * doesn't get the secret key. If it did, it would be able to decrypt all messages from cluster members!
     */
    public void nonMemberGetsSecretKeyFromKeyServer() throws Exception {
        Util.close(rogue);

        rogue=new JChannel(getTestStack());
        DISCARD discard=new DISCARD();
        discard.setDiscardAll(true);
        rogue.getProtocolStack().insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
        CustomENCRYPT encrypt=new CustomENCRYPT();
        encrypt.setProperties(new Properties());
        encrypt.init();

        rogue.getProtocolStack().insertProtocol(encrypt, ProtocolStack.BELOW, NAKACK.class);
        rogue.connect(cluster_name); // creates a singleton cluster

        assert rogue.getView().size() == 1;
        GMS gms=(GMS)rogue.getProtocolStack().findProtocol(GMS.class);
        Vector<Address> members = new Vector<Address>();
        members.add(a.getLocalAddress());
        members.add(b.getLocalAddress());
        members.add(c.getLocalAddress());
        members.add(rogue.getLocalAddress());
        View rogue_view=new View(a.getLocalAddress(), a.getView().getVid().getId(), members);
        gms.installView(rogue_view);


        // now fabricate a KEY_REQUEST message and send it to the key server (A)
        Message newMsg=new Message(a.getLocalAddress(), rogue.getLocalAddress(), encrypt.getKeyPair().getPublic().getEncoded());
        newMsg.putHeader(encrypt.getName(),new EncryptHeader(EncryptHeader.SECRET_KEY_REQ, encrypt.getSymVersion()));

        discard.setDiscardAll(false);
        System.out.printf("-- sending KEY_REQUEST to key server %s\n", a.getLocalAddress());
        encrypt.getDownProtocol().down(new Event(Event.MSG, newMsg));
        for(int i=0; i < 10; i++) {
            SecretKey secret_key=encrypt.key;
            if(secret_key != null)
                break;
            Util.sleep(500);
        }

        discard.setDiscardAll(true);
        Vector<Address> rogueMember = new Vector<Address>();
        rogueMember.add(rogue.getLocalAddress());
        gms.installView(new View(rogue.getLocalAddress(), 20, rogueMember));
        System.out.printf("-- secret key is %s (should be null)\n", encrypt.key);
        assert encrypt.key == null : String.format("should not have received secret key %s", encrypt.key);
    }



    /** Verifies that a non-member (non-coord) cannot send a JOIN-RSP to a member */
    public void nonMemberInjectingJoinResponse() throws Exception {
        Util.close(rogue);
        rogue=create();
        ProtocolStack stack=rogue.getProtocolStack();
        AUTH auth=(AUTH)stack.findProtocol(AUTH.class);
        auth.setAuthToken(new MD5Token("unknown_pwd"));
        GMS gms=(GMS)stack.findProtocol(GMS.class);
        gms.setMaxJoinAttempts(1);
        DISCARD discard=new DISCARD();
        discard.setDiscardAll(true);
        stack.insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
        rogue.connect(cluster_name);
        assert rogue.getView().size() == 1;
        discard.setDiscardAll(false);
        stack.removeProtocol("NAKACK");
        stack.removeProtocol("UNICAST");

        Vector<Address> members = new Vector<Address>();
        members.add(a.getLocalAddress());
        members.add(b.getLocalAddress());
        members.add(c.getLocalAddress());
        members.add(rogue.getLocalAddress());
        View rogue_view=new View(a.getLocalAddress(), a.getView().getVid().getId() +5, members);
        JoinRsp join_rsp=new JoinRsp(rogue_view, null);
        GMS.GmsHeader gms_hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP, join_rsp);
        Message rogue_join_rsp=new Message(b.getLocalAddress(), rogue.getLocalAddress(), null);
        rogue_join_rsp.putHeader(GMS.name, gms_hdr);
        rogue_join_rsp.setFlag(Message.NO_RELIABILITY); // bypasses NAKACK / UNICAST
        rogue.down(new Event(Event.MSG, rogue_join_rsp));
        for(int i=0; i < 10; i++) {
            if(b.getView().size() > 3)
                break;
            Util.sleep(500);
        }
        assert b.getView().size() == 3 : String.format("B's view is %s, but should be {A,B,C}", b.getView());
    }



    /** The rogue node has an incorrect {@link AUTH} config (secret) and can thus not join */
    public void rogueMemberCannotJoinDueToAuthRejection() throws Exception {
        Util.close(rogue);
        rogue=create();
        AUTH auth=(AUTH)rogue.getProtocolStack().findProtocol(AUTH.class);
        auth.setAuthToken(new MD5Token("unknown_pwd"));
        GMS gms=(GMS)rogue.getProtocolStack().findProtocol(GMS.class);
        gms.setMaxJoinAttempts(2);
        rogue.connect(cluster_name);
        System.out.printf("Rogue's view is %s\n", rogue.getView());
        assert rogue.getView().size() == 1 : String.format("rogue should have a singleton view of itself, but doesn't: %s", rogue.getView());
    }


    public void mergeViewInjectionByNonMember() throws Exception {
        Util.close(rogue);
        rogue=create();
        AUTH auth=(AUTH)rogue.getProtocolStack().findProtocol(AUTH.class);
        auth.setAuthToken(new MD5Token("unknown_pwd"));
        GMS gms=(GMS)rogue.getProtocolStack().findProtocol(GMS.class);
        gms.setMaxJoinAttempts(1);
        rogue.connect(cluster_name);

        Vector<Address> members = new Vector<Address>();
        members.add(a.getLocalAddress());
        members.add(b.getLocalAddress());
        members.add(c.getLocalAddress());
        members.add(rogue.getLocalAddress());
        MergeView merge_view=new MergeView(a.getLocalAddress(), a.getView().getVid().getId()+5, members, null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW, merge_view);
        Message merge_view_msg=new Message(null);
        merge_view_msg.putHeader(GMS.name, hdr);
        merge_view_msg.setFlag(Message.NO_RELIABILITY);
        System.out.printf("** %s: trying to install MergeView %s in all members\n", rogue.getLocalAddress(), merge_view);
        rogue.down(new Event(Event.MSG, merge_view_msg));

        // check if A, B or C installed the MergeView sent by rogue:
        for(int i=0; i < 10; i++) {
            boolean rogue_views_installed=false;

            for(JChannel ch: Arrays.asList(a,b,c))
                if(ch.getView().containsMember(rogue.getLocalAddress()))
                    rogue_views_installed=true;
            if(rogue_views_installed)
                break;
            Util.sleep(500);
        }
        for(JChannel ch: Arrays.asList(a,b,c))
            System.out.printf("%s: %s\n", ch.getLocalAddress(), ch.getView());
        for(JChannel ch: Arrays.asList(a,b,c))
            assert !ch.getView().containsMember(rogue.getLocalAddress());
    }


    /** Tests that when {ABC} -> {AB}, neither A nor B can receive a message from non-member C */
    public void testMessagesByLeftMember() throws Exception {
        Vector<Address> members = new Vector<Address>();
        members.add(a.getLocalAddress());
        members.add(b.getLocalAddress());
        View view=new View(a.getLocalAddress(), a.getView().getVid().getId()+1, members);
        for(JChannel ch: Arrays.asList(a,b)) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        };
        printView(a,b,c);
        c.getProtocolStack().removeProtocol("NAKACK"); // to prevent A and B from discarding C as non-member

        Util.sleep(1000); // give members time to handle the new view
        c.send(new Message(null, null, "hello world from left member C!"));
        for(int i=0; i < 10; i++) {
            if(ra.size() > 0 || rb.size() > 0)
                break;
            Util.sleep(500);
        }
        assert ra.size() == 0 : String.format("A: received msgs from non-member C: %s", print(ra.list()));
        assert rb.size() == 0 : String.format("B: received msgs from non-member C: %s", print(rb.list()));
    }

    /** Tests that a left member C cannot decrypt messages from the cluster */
    public void testEavesdroppingByLeftMember() throws Exception {
        printSymVersion(a,b,c);
        Vector<Address> members = new Vector<Address>();
        members.add(a.getLocalAddress());
        members.add(b.getLocalAddress());
        View view=new View(a.getLocalAddress(), a.getView().getVid().getId()+1, members);
         for(JChannel ch: Arrays.asList(a,b)) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        };
        printView(a,b,c);
        c.getProtocolStack().removeProtocol("NAKACK"); // to prevent A and B from discarding C as non-member
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a,b);

        // somewhat of a kludge as we don't have UNICAST: if we didn't remove C's connection to A, C might retransmit
        // the JOIN-REQ to A and get added to the cluster, so the code below would fail as C would be able to eavesdrop
        // on A and B
        UNICAST uni=(UNICAST)c.getProtocolStack().findProtocol(UNICAST.class);
        uni.removeConnection(a.getLocalAddress());
        Util.sleep(5000); // give members time to handle the new view

        printView(a,b,c);
        printSymVersion(a,b,c);
        a.send(new Message(null, null, "hello from A"));
        b.send(new Message(null, null, "hello from B"));

        for(int i=0; i < 10; i++) {
            if(rc.size() > 0)
                break;
            Util.sleep(500);
        }
        assert rc.size() == 0 : String.format("C: received msgs from cluster: %s", print(rc.list()));
    }


    protected JChannel create() throws Exception {
        JChannel ch=new JChannel(getTestStack());
        ProtocolStack stack=ch.getProtocolStack();
        EncryptBase encrypt=createENCRYPT();
        stack.insertProtocol(encrypt, ProtocolStack.BELOW, NAKACK.class);
        AUTH auth=new AUTH();
        auth.setAuthCoord(true);
        auth.setAuthToken(new MD5Token("mysecret")); // .setAuthCoord(false);
        stack.insertProtocol(auth, ProtocolStack.BELOW, GMS.class);
        GMS gms = (GMS)stack.findProtocol(GMS.class);
        gms.setJoinTimeout(1000); // .setValue("view_ack_collection_timeout", 10);
        STABLE stable=((STABLE)stack.findProtocol(STABLE.class));
        stable.setDesiredAverageGossip(1000);
        stable.setMaxBytes(500);
        return ch;
    }

    protected void printSymVersion(JChannel ... channels) {
        for(JChannel ch: channels) {
            ASYM_ENCRYPT encr=(ASYM_ENCRYPT)ch.getProtocolStack().findProtocol(ASYM_ENCRYPT.class);
            byte[] sym_version=encr.getSymVersion();
            System.out.printf("sym-version %s: %s\n", ch.getLocalAddress(), Util.byteArrayToHexString(sym_version));
        }
    }

    protected void printView(JChannel ... channels) {
        for(JChannel ch: channels)
            System.out.printf("%s: %s\n", ch.getLocalAddress(), ch.getView());
    }


    // Note that setting encrypt_entire_message to true is critical here, or else some of the tests in this
    // unit test would fail!
    protected ASYM_ENCRYPT createENCRYPT() throws Exception {
        ASYM_ENCRYPT encrypt=new ASYM_ENCRYPT();
        Properties props=new Properties();
        props.put("encrypt_entire_message", "true");
        props.put("sign_msgs", "true");
        encrypt.setProperties(props);
        encrypt.init();
        return encrypt;
    }



    protected static class CustomENCRYPT extends ASYM_ENCRYPT {
        protected SecretKey key;

        protected Object handleUpEvent(Message msg, EncryptHeader hdr) {
            if(hdr.getType() == EncryptHeader.SECRET_KEY_RSP) {
                try {
                    key=decodeKey(msg.getBuffer());
                    System.out.printf("received secret key %s !\n", key);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            return super.handleUpEvent(msg, hdr);
        }
    }
}
