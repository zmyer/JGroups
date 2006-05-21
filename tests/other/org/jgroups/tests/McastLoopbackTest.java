package org.jgroups.tests;

import java.net.*;

/**
 * @author Bela Ban Dec 19
 * @author 2003
 * @version $Id: McastLoopbackTest.java,v 1.3.4.1 2006/05/21 09:37:17 mimbert Exp $
 */
public class McastLoopbackTest {

    public static void main(String[] args) {
        byte[] recv_buf=new byte[1024], send_buf="Bela Ban".getBytes();
        MulticastSocket mcast_sock;
        String group_name="230.1.2.3";
        int mcast_port=7500;
        InetAddress mcast_addr, local_addr;
        int local_port;
        InetAddress bind_interface;
        DatagramPacket send_packet, recv_packet;

        if(args.length != 1) {
            System.out.println("McastTest <bind interface>");
            return;
        }

        try {
            bind_interface=InetAddress.getByName(args[0]);
            if(bind_interface == null) {
                System.err.println("bind interface " + args[0] + " not found");
                return;
            }

            local_addr=InetAddress.getByName(args[0]);
            local_port=0;
            System.out.println("local_addr=" + local_addr + ":" + local_port);

            mcast_addr=InetAddress.getByName(group_name);

            mcast_sock=new MulticastSocket(local_port);

            local_addr=mcast_sock.getLocalAddress();
            mcast_sock.setTimeToLive(32);
            // mcast_sock.setLoopbackMode(false);

            System.out.println("mcast_sock: local addr=" + mcast_sock.getLocalAddress() + ":" + mcast_sock.getLocalPort() +
                    ", interface=" + mcast_sock.getInterface());

            mcast_sock.setInterface(InetAddress.getByName(args[0]));
            System.out.println("mcast_sock: local addr=" + mcast_sock.getLocalAddress() + ":" + mcast_sock.getLocalPort() +
                    ", interface=" + mcast_sock.getInterface());

            System.out.println("-- joining " + mcast_addr + ":" + mcast_port);
            mcast_sock.joinGroup(mcast_addr);

            System.out.println("mcast_sock: local addr=" + mcast_sock.getLocalAddress() + ":" + mcast_sock.getLocalPort() +
                    ", interface=" + mcast_sock.getInterface());


            send_packet=new DatagramPacket(send_buf, send_buf.length, mcast_addr, mcast_port);
            recv_packet=new DatagramPacket(recv_buf, recv_buf.length);

            mcast_sock.send(send_packet);
            mcast_sock.receive(recv_packet);
            System.out.println("-- received " + new String(recv_packet.getData(), 0, 8) +
                    " from " + recv_packet.getAddress() + ":" + recv_packet.getPort());
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
