package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Super class of symmetric ({@link SYM_ENCRYPT}) and asymmetric ({@link ASYM_ENCRYPT}) encryption protocols.
 * @author Bela Ban
 */
public abstract class EncryptBase extends Protocol {
    protected static final String DEFAULT_SYM_ALGO="AES";


    /* -----------------------------------------    Properties     -------------------------------------------------- */
    // Cryptographic Service Provider
    protected String                        provider;

    // Cipher engine transformation for asymmetric algorithm. Default is RSA
    protected String                        asym_algorithm="RSA";

    // Cipher engine transformation for symmetric algorithm. Default is AES
    protected String                        sym_algorithm=DEFAULT_SYM_ALGO;

    // Initial public/private key length. Default is 512
    protected int                           asym_keylength=512;

    // Initial key length for matching symmetric algorithm. Default is 128
    protected int                           sym_keylength=128;

    // Number of ciphers in the pool to parallelize encrypt and decrypt requests
    protected int                           cipher_pool_size=8;

    // If true, the entire message (including payload and headers) is encrypted, else only the payload
    protected boolean                       encrypt_entire_message=true;

    // If true, all messages are digitally signed by adding an encrypted checksum of the encrypted
    // message to the header. Ignored if encrypt_entire_message is false
    protected boolean                       sign_msgs=true;

    // When sign_msgs is true, by default CRC32 is used to create the checksum. If use_adler is
    // true, Adler32 will be used
    protected boolean                       use_adler;

    protected volatile Address              local_addr;

    protected volatile View                 view;

    // Cipher pools used for encryption and decryption. Size is cipher_pool_size
    protected BlockingQueue<Cipher>         encoding_ciphers, decoding_ciphers;

    // version filed for secret key
    protected volatile byte[]               sym_version;

    // shared secret key to encrypt/decrypt messages
    protected volatile SecretKey            secret_key;

    // map to hold previous keys so we can decrypt some earlier messages if we need to
    protected final Map<AsciiString,Cipher> key_map=new WeakHashMap<AsciiString,Cipher>();

    public int       getAsymKeylength()                 {return asym_keylength;}
    public void      setAsymKeylength(int len)          {this.asym_keylength=len;}
    public int       getSymKeylength()                  {return sym_keylength;}
    public void      setSymKeylength(int len)           {this.sym_keylength=len;}
    public SecretKey getSecretKey()                     {return secret_key;}
    public void      setSecretKey(SecretKey key)        {this.secret_key=key;}
    public String    getSymAlgorithm()                  {return sym_algorithm;}
    public void      setSymAlgorithm(String alg)        {this.sym_algorithm=alg;}
    public String    getAsymAlgorithm()                 {return asym_algorithm;}
    public void      setAsymAlgorithm(String alg)       {this.asym_algorithm=alg;}
    public byte[]    getSymVersion()                    {return sym_version;}
    public void      setSymVersion(byte[] v)            {this.sym_version=Arrays.copyOf(v, v.length);}
    public void      setLocalAddress(Address addr)      {this.local_addr=addr;}
    public boolean   getEncryptEntireMessage()          {return encrypt_entire_message;}
    public void      setEncryptEntireMessage(boolean b) {this.encrypt_entire_message=b;}
    public boolean   getSignMessages()                  {return this.sign_msgs;}
    public void      setSignMessages(boolean flag)      {this.sign_msgs=flag;}
    public boolean   getAdler()                         {return use_adler;}
    public void      setAdler(boolean flag)             {this.use_adler=flag;}
    public String    getVersion()                       {return Util.byteArrayToHexString(sym_version);}

    public boolean setProperties(Properties props)
    {
        String str;

        super.setProperties(props);

        str = props.getProperty("provider");
        if (str != null)
        {
            provider = str;
            props.remove("provider");

            if (log.isInfoEnabled())
                log.info("Provider used is " + provider);
        }

        // asymmetric algorithm name
        str = props.getProperty("asym_algorithm");
        if (str != null)
        {
            asym_algorithm = str;
            props.remove("asym_algorithm");

            if (log.isInfoEnabled())
                log.info("Asym algo used is " + asym_algorithm);
        }

        // symmetric algorithm name
        str = props.getProperty("sym_algorithm");
        if (str != null)
        {
            sym_algorithm = str;
            props.remove("sym_algorithm");

            if (log.isInfoEnabled())
                log.info("Sym algo used is " + sym_algorithm);
        }

        // asymmetric key length
        str = props.getProperty("asym_keylength");
        if (str != null)
        {
            asym_keylength = Integer.parseInt(str);
            props.remove("asym_keylength");

            if (log.isInfoEnabled())
                log.info("Asym keylength used is " + asym_keylength);
        }

        // asymmetric key length
        str = props.getProperty("sym_keylength");
        if (str != null)
        {
            sym_keylength = Integer.parseInt(str);
            props.remove("sym_keylength");

            if (log.isInfoEnabled())
                log.info("Sym keylength used is " + sym_keylength);
        }

        // cipher pool size
        str = props.getProperty("cipher_pool_size");
        if (str != null)
        {
            cipher_pool_size = Integer.parseInt(str);
            props.remove("cipher_pool_size");
        }

        str=props.getProperty("encrypt_entire_message");
        if(str != null)
        {
            encrypt_entire_message=Boolean.valueOf(str).booleanValue();
            props.remove("encrypt_entire_message");
        }

        str=props.getProperty("sign_msgs");
        if(str != null)
        {
            sign_msgs=Boolean.valueOf(str).booleanValue();
            props.remove("sign_msgs");
        }

        str=props.getProperty("use_adler");
        if(str != null)
        {
            use_adler=Boolean.valueOf(str).booleanValue();
            props.remove("use_adler");
        }

        return true;
    }


    public void init() throws Exception {
        int tmp=Util.getNextHigherPowerOfTwo(cipher_pool_size);
        if(tmp != cipher_pool_size) {
            log.warn(String.format("%s: setting cipher_pool_size (%d) to %d (power of 2) for faster modulo operation", local_addr, cipher_pool_size, tmp));
            cipher_pool_size=tmp;
        }
        encoding_ciphers=new ArrayBlockingQueue<Cipher>(cipher_pool_size);
        decoding_ciphers=new ArrayBlockingQueue<Cipher>(cipher_pool_size);
        initSymCiphers(sym_algorithm, secret_key);
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                try {
                    if(secret_key == null) {
                        log.trace(String.format("%s: discarded %s message to %s as secret key is null, hdrs: %s",
                                  local_addr, msg.getDest() == null? "mcast" : "unicast", msg.getDest(), msg.printHeaders()));
                        return null;
                    }
                    encryptAndSend(msg);
                }
                catch(Exception e) {
                    log.warn(local_addr + ": unable to send message down", e);
                }
                return null;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                try {
                    return handleUpMessage(msg);
                }
                catch(Exception e) {
                    log.warn(local_addr + ": exception occurred decrypting message", e);
                }
                return null;
        }
        return up_prot.up(evt);
    }




    /** Initialises the ciphers for both encryption and decryption using the generated or supplied secret key */
    protected synchronized void initSymCiphers(String algorithm, SecretKey secret) throws Exception {
        if(secret == null)
            return;
        encoding_ciphers.clear();
        decoding_ciphers.clear();
        for(int i=0; i < cipher_pool_size; i++ ) {
            encoding_ciphers.add(createCipher(Cipher.ENCRYPT_MODE, secret, algorithm));
            decoding_ciphers.add(createCipher(Cipher.DECRYPT_MODE, secret, algorithm));
        };

        //set the version
        MessageDigest digest=MessageDigest.getInstance("MD5");
        digest.reset();
        digest.update(secret.getEncoded());

        byte[] tmp=digest.digest();
        sym_version=Arrays.copyOf(tmp, tmp.length);
        log.debug(String.format("%s: created %d symmetric ciphers with secret key (%d bytes)", local_addr, cipher_pool_size, sym_version.length));
    }


    protected Cipher createCipher(int mode, SecretKey secret_key, String algorithm) throws Exception {
        Cipher cipher=provider != null && !provider.trim().isEmpty()?
          Cipher.getInstance(algorithm, provider) : Cipher.getInstance(algorithm);
        cipher.init(mode, secret_key);
        return cipher;
    }


    protected Object handleUpMessage(Message msg) throws Exception {
        EncryptHeader hdr=(EncryptHeader)msg.getHeader(getName());
        if(hdr == null) {
            log.error(String.format("%s: received message without encrypt header from %s; dropping it", local_addr, msg.getSrc()));
            return null;
        }
        switch(hdr.getType()) {
            case EncryptHeader.ENCRYPT:
                return handleEncryptedMessage(msg);
            default:
                return handleUpEvent(msg,hdr);
        }
    }


    protected Object handleEncryptedMessage(Message msg) throws Exception {
        if(!process(msg))
            return null;

        // try and decrypt the message - we need to copy msg as we modify its
        // buffer (http://jira.jboss.com/jira/browse/JGRP-538)
        Message tmpMsg=decryptMessage(null, msg.copy()); // need to copy for possible xmits
        if(tmpMsg != null)
            return up_prot.up(new Event(Event.MSG, tmpMsg));
        log.warn(String.format("%s: unrecognized cipher; discarding message from %s", local_addr, msg.getSrc()));
        return null;
    }

    protected Object handleUpEvent(Message msg, EncryptHeader hdr) {
        return null;
    }

    /** Whether or not to process this received message */
    protected boolean process(Message msg) {return true;}

    protected void handleView(View view) {
        this.view=view;
    }

    protected boolean inView(Address sender, String error_msg) {
        View curr_view=this.view;
        if(curr_view == null || curr_view.containsMember(sender))
            return true;
        log.error(String.format(error_msg, local_addr, sender, curr_view));
        return false;
    }

    protected Checksum createChecksummer() {return use_adler? new Adler32() : new CRC32();}


    /** Does the actual work for decrypting - if version does not match current cipher then tries the previous cipher */
    protected Message decryptMessage(Cipher cipher, Message msg) throws Exception {
        EncryptHeader hdr=(EncryptHeader)msg.getHeader(getName());
        if(hdr.getVersion() == null)
            return null;
        if(!Arrays.equals(hdr.getVersion(), sym_version)) {
            cipher=key_map.get(new AsciiString(hdr.getVersion()));
            if(cipher == null) {
                handleUnknownVersion();
                return null;
            }
            log.trace(String.format("%s: decrypting msg from %s using previous cipher version", local_addr, msg.getSrc()));
            return _decrypt(cipher, msg, hdr);
        }
        return _decrypt(cipher, msg, hdr);
    }

    protected Message _decrypt(final Cipher cipher, Message msg, EncryptHeader hdr) throws Exception {
        byte[] decrypted_msg;

        if(!encrypt_entire_message && msg.getLength() == 0)
            return msg;

        if(encrypt_entire_message && sign_msgs) {
            byte[] signature=hdr.getSignature();
            if(signature == null) {
                log.error(String.format("%s: dropped message from %s as the header did not have a checksum", local_addr, msg.getSrc()));
                return null;
            }

            long msg_checksum=decryptChecksum(cipher, signature, 0, signature.length);
            long actual_checksum=computeChecksum(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            if(actual_checksum != msg_checksum) {
                log.error(String.format("%s: dropped message from %s as the message's checksum (%d) did not match the computed checksum (%d)",
                          local_addr, msg.getSrc(), msg_checksum, actual_checksum));
                return null;
            }
        }

        if(cipher == null)
            decrypted_msg=code(msg.getRawBuffer(), msg.getOffset(), msg.getLength(), true);
        else
            decrypted_msg=cipher.doFinal(msg.getRawBuffer(), msg.getOffset(), msg.getLength());

        if(!encrypt_entire_message) {
            msg.setBuffer(decrypted_msg);
            return msg;
        }

        Message ret=(Message)Util.streamableFromByteBuffer(Message.class,decrypted_msg,0,decrypted_msg.length);
        if(ret.getDest() == null)
            ret.setDest(msg.getDest());
        if(ret.getSrc() == null)
            ret.setSrc(msg.getSrc());
        return ret;
    }


    protected void encryptAndSend(Message msg) throws Exception {
        EncryptHeader hdr=new EncryptHeader(EncryptHeader.ENCRYPT, getSymVersion());
        if(encrypt_entire_message) {
            if(msg.getSrc() == null)
                msg.setSrc(local_addr);

            byte [] serialized_msg=Util.streamableToByteBuffer(msg);
            byte[] encrypted_msg=code(serialized_msg,0,serialized_msg.length,false);

            if(sign_msgs) {
                long checksum=computeChecksum(encrypted_msg, 0, encrypted_msg.length);
                byte[] checksum_array=encryptChecksum(checksum);
                hdr.setSignature(checksum_array);
            }

            // exclude existing headers, they will be seen again when we decrypt and unmarshal the msg at the receiver
            Message tmp=msg.copy(false, false);
            tmp.setBuffer(encrypted_msg);
            tmp.putHeader(getName(),hdr);
            down_prot.down(new Event(Event.MSG, tmp));
            return;
        }

        // copy neeeded because same message (object) may be retransmitted -> prevent double encryption
        Message msgEncrypted=msg.copy(false);
        msgEncrypted.putHeader(getName(), hdr);
        if(msg.getLength() > 0)
            msgEncrypted.setBuffer(code(msg.getRawBuffer(),msg.getOffset(),msg.getLength(),false));
        down_prot.down(new Event(Event.MSG,msgEncrypted));
    }


    protected byte[] code(byte[] buf, int offset, int length, boolean decode) throws Exception {
        BlockingQueue<Cipher> queue=decode? decoding_ciphers : encoding_ciphers;
        Cipher cipher=queue.take();
        try {
            return cipher.doFinal(buf, offset, length);
        }
        finally {
            queue.offer(cipher);
        }
    }

    protected long computeChecksum(byte[] input, int offset, int length) {
        Checksum checksummer=createChecksummer();
        checksummer.update(input, offset, length);
        return checksummer.getValue();
    }

    protected byte[] encryptChecksum(long checksum) throws Exception {
        byte[] checksum_array=new byte[Global.LONG_SIZE];
        Bits.writeLong(checksum, checksum_array, 0);
        return code(checksum_array, 0, checksum_array.length, false);
    }

    protected long decryptChecksum(final Cipher cipher, byte[] input, int offset, int length) throws Exception {
        byte[] decrypted_checksum;
        if(cipher == null)
            decrypted_checksum=code(input, offset, length, true);
        else
            decrypted_checksum=cipher.doFinal(input, offset, length);
        return Bits.readLong(decrypted_checksum, 0);
    }


    /* Get the algorithm name from "algorithm/mode/padding"  taken from original ENCRYPT */
    protected static String getAlgorithm(String s) {
        int index=s.indexOf('/');
        return index == -1? s : s.substring(0, index);
    }


    /** Called when the version shipped in the header can't be found */
    protected void handleUnknownVersion() {}



}
