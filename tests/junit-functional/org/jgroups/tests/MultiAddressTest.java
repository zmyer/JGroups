package org.jgroups.tests;

import org.testng.annotations.Test;
import org.jgroups.Global;
import org.jgroups.MultiAddress;
import org.jgroups.Address;
import org.jgroups.util.UUID;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

/**
 * @author Bela Ban
 * @version $Id: MultiAddressTest.java,v 1.1.2.1 2009/02/24 12:20:23 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class MultiAddressTest {

    public static void testEquals() {
        MultiAddress a1, a2;
        List<Address> l1=new ArrayList<Address>();
        List<Address> l2=new ArrayList<Address>();
        Collections.addAll(l1, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        Collections.addAll(l2, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        a1=new MultiAddress(l1, "cluster");
        a2=new MultiAddress(l2, "cluster");
        assert a1.equals(a2);
        assert a1.compareTo(a2) == 0;

        a2=new MultiAddress(l2, "cluster-2");
        assert !a1.equals(a2);
        assert a1.compareTo(a2) != 0;
    }

    public static void testHashCode() {
        MultiAddress a1, a2;
        List<Address> l1=new ArrayList<Address>();
        List<Address> l2=new ArrayList<Address>();
        Collections.addAll(l1, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        Collections.addAll(l2, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        a1=new MultiAddress(l1, "cluster");
        a2=new MultiAddress(l2, "cluster");
        assert a1.hashCode() == a2.hashCode();

        a2=new MultiAddress(l2, "cluster-2");
        assert a1.hashCode() != a2.hashCode();
    }
}
