package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.Properties;

/**
 * Tests use cases for {@link SYM_ENCRYPT} described in https://issues.jboss.org/browse/JGRP-2021.
 * Make sure you create the keystore before running this test (ant make-keystore).
 * @author Bela Ban
 * @since  4.0
 */
public class SYM_ENCRYPT_Test extends EncryptTest {
    protected static final String DEF_PWD="changeit";
 
    protected String getProtocolName()
    {
        return "SYM_ENCRYPT";
    }

    protected JChannel create() throws Exception {
        JChannel ch=new JChannel(getTestStack());
        SYM_ENCRYPT encrypt;
        try {
            encrypt=createENCRYPT("keystore/defaultStore.keystore", DEF_PWD);
        }
        catch(Throwable t) {
            encrypt=createENCRYPT("defaultStore.keystore", DEF_PWD);
        }
        ch.getProtocolStack().insertProtocol(encrypt, ProtocolStack.BELOW, NAKACK.class);
        return ch;
    }

    // Note that setting encrypt_entire_message to true is critical here, or else some of the tests in this
    // unit test would fail!
    protected SYM_ENCRYPT createENCRYPT(String keystore_name, String store_pwd) throws Exception {
        SYM_ENCRYPT encrypt=new SYM_ENCRYPT();
        Properties props=new Properties();
        props.put("keystore_name", keystore_name);
        props.put("alias", "myKey");
        props.put("store_password", store_pwd);
        props.put("encrypt_entire_message", "true");
        props.put("sign_msgs", "true");
        encrypt.setProperties(props);
        encrypt.init();
        return encrypt;
    }
}
