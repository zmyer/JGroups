package org.jgroups.protocols.jentMESH;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.jentMESH.TREEMESH.ConnectionManager;
import org.jgroups.protocols.jentMESH.dataTypes.ConnectionType;
import org.jgroups.protocols.jentMESH.util.Clock;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;

/**
 * Protocol that works with TREEMESH to timeout nodes, detect partitions, 
 * and perform mergers after partitions occur.
 *
 * @author Mike Jensen
 */
public class TREEFD extends TreeProtocol {
  /* ------------------------------------------    Properties     ---------------------------------------------- */
  @Property(description="How frequently in milliseconds should the node broadcast it's heartbeat", writable=false)
  protected short broadcastIntravel = 500;
  
  @Property(description="How long till we timeout a silent node, should be above the heartbeat in TREEMESH")
  protected int nodeTimeout = 10 * broadcastIntravel;
  
  @Property(description="Time delay between regular process calls in milliseconds", writable=false)
  protected long processPeriod = 5000;

  @Property(description="Min free memory for heartbeat, " +
  		                  "so node will timeout if all memory is consumed, set to zero to disable")
  protected long minFreeMemory = 1024 * 1024 * 1;  // 1 megabyte
  
  /* ---------------------------------------------    Fields    ------------------------------------------------ */
  private final Log log = LogFactory.getLog(getClass());
  private final List<Future<?>> scheduledTasks = new ArrayList<Future<?>>(2); // currently 2 scheduled tasks
  protected TimeScheduler timer;
  
  @Override
  public void init() {
    final TREEMESH tm = ((TREEMESH)this.stack.findProtocol(TREEMESH.class));
    
    timer = getTransport().getTimer();
    
    // schedule a thread to do regular tasks
    Future<?> regularProcess = timer.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        boolean timeoutOccured = tm.getMeshModel().timeoutDeadNodes(nodeTimeout);
        if (timeoutOccured) {
          tm.sendNewView();
        }
        
        // we have to do this entire merge bit in the connection manager to ensure we can maintain the connection_lock
        tm.getConnectionManager().mergeCheckAndMaybeMerge();
      }
    }, nodeTimeout, processPeriod, TimeUnit.MILLISECONDS);
    scheduledTasks.add(regularProcess);
    
    // schedule a thread to regularly send the heartbeat
    Future<?> heartBeat = timer.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (enoughRamResources()) {
          tm.sendHeartbeat();
        } else {
          System.gc();  // try to GC then try one last ditch attempt to see if we can heartbeat
          if (enoughRamResources()) {
            tm.sendHeartbeat();
          } else {
            if (log.isFatalEnabled()) {
              log.fatal("Unable to send heartbeat because of memory allocation checks failing");
            }
          }
        }
      }
      
      private boolean enoughRamResources() {
        return minFreeMemory == 0 || Runtime.getRuntime().freeMemory() > minFreeMemory;
      }
    }, 500, broadcastIntravel, TimeUnit.MILLISECONDS);
    scheduledTasks.add(heartBeat);
  }
  
  @Override
  public void destroy() {
    // Stop all scheduled tasks
    Iterator<Future<?>> it = scheduledTasks.iterator();
    while (it.hasNext()) {
      it.next().cancel(false);
    }
  }
}
