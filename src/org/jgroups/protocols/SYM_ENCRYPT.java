package org.jgroups.protocols;

import org.jgroups.util.Util;

import javax.crypto.SecretKey;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Properties;

/**
 * Encrypts and decrypts communication in JGroups by using a secret key shared by all cluster members.<p>
 *
 * The secret key is identical for all cluster members and is injected into this protocol at startup, e.g. by reading
 * it from a keystore. Messages are sent by encrypting them with the secret key and received by decrypting them with
 * the secret key. Note that all cluster members must be shipped with the same keystore file<p>
 *
 * This protocol is typically placed under {@link org.jgroups.protocols.pbcast.NAKACK}, so that most important
 * headers are encrypted as well, to prevent replay attacks.<p>
 *
 * A possible configuration looks like this:<br><br>
 * {@code <SYM_ENCRYPT key_store_name="defaultStore.keystore" store_password="changeit" alias="myKey"/>}
 * <br>
 * <br>
 * In order to use SYM_ENCRYPT layer in this manner, it is necessary to have the secret key already generated in a
 * keystore file. The directory containing the keystore file must be on the application's classpath. You cannot create a
 * secret key keystore file using the keytool application shipped with the JDK. A java file called KeyStoreGenerator is
 * included in the demo package that can be used from the command line (or IDE) to generate a suitable keystore.
 *
 * @author Bela Ban
 * @author Steve Woodcock
 */
public class SYM_ENCRYPT extends EncryptBase {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    // File on classpath that contains keystore repository
    protected String   keystore_name;

    // Password used to check the integrity/unlock the keystore. Change the default
    protected String   store_password="changeit"; // JDK default

    // Password for recovering the key. Change the default"
    protected String   key_password; // allows to assign keypwd=storepwd if not set (https://issues.jboss.org/browse/JGRP-1375)


    // Alias used for recovering the key. Change the default"
    protected String   alias="mykey"; // JDK default


    public String getKeystoreName()                      {return this.keystore_name;}
    public void   setKeystoreName(String n)              {this.keystore_name=n;}
    public String getAlias()                             {return alias;}
    public void   setAlias(String a)                     {this.alias=a;}
    public String getStorePassword()                     {return store_password;}
    public void   setStorePassword(String pwd)           {this.store_password=pwd;}


    public String getName()
    {
        return "SYM_ENCRYPT";
    }

    public boolean setProperties(Properties props)
    {
        String str;

        super.setProperties(props);

        // key store name
        str = props.getProperty("keystore_name");
        if (str != null)
        {
            keystore_name = str;
            props.remove("keystore_name");

            if (log.isInfoEnabled())
                log.info("keystore_name used is " + keystore_name);
        }

        // key store password
        str = props.getProperty("store_password");
        if (str != null)
        {
            store_password = str;
            props.remove("store_password");

            if (log.isInfoEnabled())
                log.info("store_password used is not null");
        }

        // key password
        str = props.getProperty("key_password");
        if (str != null)
        {
            key_password = str;
            props.remove("key_password");

            if (log.isInfoEnabled())
                log.info("key_password used is not null");
        } else if (store_password != null)
        {
            key_password = store_password;

            if (log.isInfoEnabled())
                log.info("key_password used is same as store_password");
        }

        // key aliase
        str = props.getProperty("alias");
        if (str != null)
        {
            alias = str;
            props.remove("alias");

            if (log.isInfoEnabled())
                log.info("alias used is " + alias);
        }

        if (!props.isEmpty())
        {

            if (log.isErrorEnabled())
                log.error("these properties are not recognized:" + props);
            return false;
        }

        return true;
    }


    public void init() throws Exception {
        readSecretKeyFromKeystore();
        super.init();
    }

    /**
     * Initialisation if a supplied key is defined in the properties. This supplied key must be in a keystore which
     * can be generated using the keystoreGenerator file in demos. The keystore must be on the classpath to find it.
     */
    protected void readSecretKeyFromKeystore() throws Exception {
        InputStream inputStream=null;
        // must not use default keystore type - as it does not support secret keys
        KeyStore store=KeyStore.getInstance("JCEKS");

        SecretKey tempKey=null;
        try {
            if(this.secret_key == null) { // in case the secret key was set before, e.g. via injection in a unit test
                // load in keystore using this thread's classloader
                inputStream=Thread.currentThread().getContextClassLoader().getResourceAsStream(keystore_name);
                if(inputStream == null)
                    inputStream=new FileInputStream(keystore_name);
                // we can't find a keystore here -
                if(inputStream == null)
                    throw new Exception("Unable to load keystore " + keystore_name + " ensure file is on classpath");
                // we have located a file lets load the keystore
                try {
                    store.load(inputStream, store_password.toCharArray());
                    // loaded keystore - get the key
                    tempKey=(SecretKey)store.getKey(alias, key_password.toCharArray());
                }
                catch(IOException e) {
                    throw new Exception("Unable to load keystore " + keystore_name + ": " + e);
                }
                catch(NoSuchAlgorithmException e) {
                    throw new Exception("No Such algorithm " + keystore_name + ": " + e);
                }
                catch(CertificateException e) {
                    throw new Exception("Certificate exception " + keystore_name + ": " + e);
                }

                if(tempKey == null)
                    throw new Exception("Unable to retrieve key '" + alias + "' from keystore " + keystore_name);
                this.secret_key=tempKey;
                if(sym_algorithm.equals(DEFAULT_SYM_ALGO))
                    sym_algorithm=tempKey.getAlgorithm();
            }
        }
        finally {
            Util.close(inputStream);
        }
    }


}
