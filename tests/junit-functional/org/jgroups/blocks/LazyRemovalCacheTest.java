package org.jgroups.blocks;

import org.testng.annotations.Test;
import org.jgroups.Global;
import org.jgroups.util.UUID;

import java.util.Arrays;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: LazyRemovalCacheTest.java,v 1.1.2.1 2009/03/04 10:04:40 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class LazyRemovalCacheTest {

    public static void testAdd() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<UUID, String>();
        UUID uuid=UUID.randomUUID();
        cache.add(uuid, "node-1");
        System.out.println("cache = " + cache);
        assert 1 == cache.size();
        String val=cache.get(uuid);
        assert val != null && val.equals("node-1");

        cache.remove(uuid);
        System.out.println("cache = " + cache);
    }

    public static void testRemoveAll() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<UUID, String>();
        List<UUID> list=Arrays.asList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        int cnt=1;
        for(UUID uuid: list)
            cache.add(uuid, "node-" + cnt++);
        UUID uuid1=UUID.randomUUID();
        UUID uuid2=UUID.randomUUID();
        cache.add(uuid1, "foo");
        cache.add(uuid2, "bar");
        System.out.println("cache = " + cache);
        assert cache.size() == 5;

        System.out.println("removing " + list);
        cache.removeAll(list);
        System.out.println("cache = " + cache);
        assert cache.size() == 5;
        assert cache.get(uuid1).equals("foo");
        assert cache.get(uuid2).equals("bar");

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 2;
        assert cache.get(uuid1).equals("foo");
        assert cache.get(uuid2).equals("bar");
    }
    
}
