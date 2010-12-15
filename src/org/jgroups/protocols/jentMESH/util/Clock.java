package org.jgroups.protocols.jentMESH.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This is a utility class for low-resolution timing which avoids
 * frequent System.currentTimeMillis() calls (which perform poorly due
 * because they require system calls).
 *
 * Each call to Clock.nowInMilliseconds() will return the value of
 * System.currentTimeMillis() as of the last call to Clock.update().
 * This means nowInMilliseconds() will only be as accurate as the
 * frequency with which update() is called.
 */
public class Clock {
  private static volatile long now = System.currentTimeMillis();
  private static final boolean log = false;
  private static long maxTimeDiff = -1;
  private static List<Long> record = new LinkedList<Long>();

  public static long nowInMilliseconds() {
    return now;
  }

  public static void update() {
    if (log) {
      long oldTime = now;
      now = System.currentTimeMillis();
      long timeDiff = now - oldTime;
      synchronized (record) {
        if (timeDiff > maxTimeDiff) {
          maxTimeDiff = timeDiff;
        }
        record.add(new Long(timeDiff));
      }
    } else {
      now = System.currentTimeMillis();
    }
  }
  
  public static String getStats() {
    long timeDiffTotal = 0;
    synchronized (record) {
      Iterator<Long> it = record.iterator();
      while (it.hasNext()) {
        timeDiffTotal += it.next();
      }
      if (record.size() > 0) {
        return "Max timeDiff: " + maxTimeDiff + ", avgDiff: " + (timeDiffTotal / record.size());
      } else {
        return "No recorded stats";
      }
    }
  }

  public static long updateAndGetCurrTime() {
    update();
    return nowInMilliseconds();
  }
}
