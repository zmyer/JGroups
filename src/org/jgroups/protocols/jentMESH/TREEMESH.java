package org.jgroups.protocols.jentMESH;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.ViewId;
import org.jgroups.annotations.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.jentMESH.dataTypes.HandshakeData;
import org.jgroups.protocols.jentMESH.dataTypes.MeshIdentifier;
import org.jgroups.protocols.jentMESH.dataTypes.MeshView;
import org.jgroups.protocols.jentMESH.dataTypes.NodeConnectionDetails;
import org.jgroups.protocols.jentMESH.dataTypes.PeerRecommendationData;
import org.jgroups.protocols.jentMESH.dataTypes.headers.MeshHeartbeatHeader;
import org.jgroups.protocols.jentMESH.dataTypes.headers.MeshHandshakeHeader;
import org.jgroups.protocols.jentMESH.dataTypes.headers.MeshMsgHeader;
import org.jgroups.protocols.jentMESH.dataTypes.headers.ConnectionClosedHeader;
import org.jgroups.protocols.jentMESH.dataTypes.headers.PeerRecommendationHeader;
import org.jgroups.protocols.jentMESH.dataTypes.headers.MeshRoutedMsgHeader;
import org.jgroups.protocols.jentMESH.dataTypes.headers.TreeMeshHeader;
import org.jgroups.protocols.jentMESH.dataTypes.ConnectionType;
import org.jgroups.protocols.jentMESH.util.Clock;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Protocol to establish groups where all nodes are not connected to all other nodes.
 * See design document located at: http://archive.jentfoo.com/coding_projects/jentMesh_design.pdf
 *
 * @author Mike Jensen
 */
@Experimental @Unsupported
@MBean(description="TREEMESH protocol")
public class TREEMESH extends TreeProtocol {
  /* ------------------------------------------    Properties     ---------------------------------------------- */
  @Property(description="Time delay between checks to establish new connections", writable=false)
  protected short connectionProcessPeriod = 10000;
  
  @Property(description="Timeout when trying to route a message")
  protected int msgRoutingTimeout = 60000;
  
  @Property(description="Timeout when trying to connect to a new node")
  protected short newConnectionTimeout = 10000;
  
  @Property(description="How long do we try to recommend a node to fix a split brain situation, " +
  		                  "before we just connect to them ourselves.")
  protected short splitNodeRecommendationTimeout = (short) Math.max(newConnectionTimeout, 
                                                                    connectionProcessPeriod * 2);

  @Property(description="Number representing how sure we want to be before we make decisions.  " +
                        "Must be at least 2!", writable=false)
  protected short sampleSize = 5;
  
  // we throw an UnsupportedOperationException if this is false and we try to send a routed msg
  @Property(description="Allow message routing, " +
                        "requires additional load to analyze good message routes", writable=false)
  protected boolean useRoutableMeshModel = true;

  @Property(description="Use advanced routing model.  " +
                        "Advanced routing does not just look at the last path, " +
                        "but tries to pick a good path over time, " +
                        "and adds in message cycle detection.", writable=false)
  protected boolean useAdvancedRoutingModel = false;
  
  @Property(description="Echo broadcast messages to ourself")
  protected boolean echoBroadcast = true;

  @Property(description="Window size to accept out of order msgs, " +
  		                  "set to 1 to NOT accept any out of order messages, can not be set below 1")
  protected int msgWindowSize = 1;

  @Property(description="Time we wait for merge after being a Root node, " +
  		                  "this prevents us from trying to merge too fast " +
  		                  "and give us a greater chance of remaining as the root node.", writable=false)
  protected long rootStickyTime = 10000; // default is 10 seconds

  @Property(description="Set to true so that heartbeat and other mesh communications are used using OOB messages.")
  protected boolean runOOB = true;  // default is to run treeMesh messages OOB from application messages

  @Property(description="Create a connection status log that can be used to draw the connection state", writable=false)
  protected boolean useMeshGUI = true;
  
  private final int MEMBER_DISCOVERY_METHOD = Event.FIND_INITIAL_MBRS;  // we should be using this always, but it is dependent on a configuration setup as well
  //protected final int MEMBER_DISCOVERY_METHOD = Event.FIND_ALL_MBRS;
  protected static boolean verbose = true;
  protected static boolean msgVerbose = verbose && false;
  protected static boolean highVerbose = verbose && false;
  
  /* ---------------------------------------------    Fields    ------------------------------------------------ */
  @ManagedAttribute
  protected volatile boolean is_coord = false;  // TODO - is_coord = Util.isCoordinator(view, local_addr);

  protected final Log log = LogFactory.getLog(getClass());
  protected final List<Future<?>> scheduledTasks = new ArrayList<Future<?>>(1); // currently 1 scheduled tasks
  protected TimeScheduler timer;
  protected final MeshIdentifier local_addr = new MeshIdentifier();
  protected long nextViewID = UUID.randomUUID().getLeastSignificantBits(); // start with something semi-unique
  protected MeshModel meshModel;
  protected ConnectionManager conManager;
  protected final Map<Address, LinkedList<Long>> broadcastHistory = new HashMap<Address, LinkedList<Long>>();
  protected boolean disconnected = false;

  @Override
  public Vector<Integer> requiredDownServices() {
      Vector<Integer> retval=new Vector<Integer>(1);
      retval.addElement(new Integer(MEMBER_DISCOVERY_METHOD));
      return retval;
  }

  @Override
  public void init() {
    // verify necessary config settings
    if (sampleSize < 2) {
      if (log.isWarnEnabled()) {
        log.warn("sampleSize config value must be at least two, resetting to minimum value");
      }
      sampleSize = 2;
    }
    Iterator<Protocol> it = this.getProtocolStack().getProtocols().iterator();
    while (it.hasNext()) {
      Protocol prot = it.next();
      if (prot instanceof Discovery) {
        Discovery disc = (Discovery) prot;
        if (disc.getNumInitialMembers() < 1000) {
          if (log.isWarnEnabled()) {
            log.warn("Discovery protocol was configured with an invalid value.  " +
            		"num_initial_members should be higher than the expected group size, we default this at 1000");
          }
          disc.setNumInitialMembers(1000);
        }
        disc.setValue("break_on_coord_rsp", new Boolean(false));
      }
    }
    
    getMeshModel(); // will construct the model if null
    getConnectionManager(); // will construct a connection manager if null
    
    timer = getTransport().getTimer();
    
    // schedule a thread to do regular tasks
    Future<?> regularProcess = timer.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (! disconnected) {
          if (! meshModel.isModelStable()) {  // don't do anything till we are stable, or we may make things worse
            if (log.isInfoEnabled() && verbose) {
              log.info("Mesh is not stable, will retry connection check later");
            }
            return;
          }
          Clock.update();
          
          conManager.maybeMakePeerConnections();
          
          conManager.maybeMakeLeafConnections();
        }
      }
    }, 1000, connectionProcessPeriod, TimeUnit.MILLISECONDS);
    scheduledTasks.add(regularProcess);
  }
  
  public MeshModel getMeshModel() {
    if (meshModel == null) {
      if (useMeshGUI) {
        meshModel = new MeshGUIModel(sampleSize, 
                                     MeshModel.getModelVal(useRoutableMeshModel, useAdvancedRoutingModel), 
                                     rootStickyTime, 
                                     (int)(Math.random() * Integer.MAX_VALUE) + "_status.txt", 
                                     local_addr, getTransport().getTimer());
      } else {
        meshModel = new MeshModel(sampleSize, 
                                  MeshModel.getModelVal(useRoutableMeshModel, useAdvancedRoutingModel),
                                  rootStickyTime);
      }
    }
    return meshModel;
  }
  
  public ConnectionManager getConnectionManager() {
    if (conManager == null) {
      conManager = new ConnectionManager();
    }
    return conManager;
  }

  @Override
  public void destroy() {
    // Stop all scheduled tasks
    Iterator<Future<?>> it = scheduledTasks.iterator();
    while (it.hasNext()) {
      it.next().cancel(false);
    }
  }

  @Override
  public void stop() {
  }

  @Override
  public Object down(Event event) {
    switch (event.getType()) {
      case Event.MSG:
        if (disconnected) {
          break;
        }
        // add our header on the message before passing on
        Message msg = (Message)event.getArg();
        //log.info("---> recieved a msg " + (msg.getSrc() != null ? (msg.getSrc().equals(local_addr) ? "from: ourself - " : "from: a remote node - ") : "- ") + (msg.getObject() != null ? msg.getObject().getClass() : null));  // TODO - remove temp logging
        
        final MeshMsgHeader mmh = new MeshMsgHeader((msg.getDest() == null || msg.getDest().isMulticastAddress() ? null : msg.getDest()),
                                                    local_addr.getAddress(), 
                                                    Clock.nowInMilliseconds(), 
                                                    meshModel.getAvgHops(), 
                                                    meshModel.peerConnectionCount(), 
                                                    meshModel.leafConnectionCount());
        
        msg.putHeaderIfAbsent(getId(), mmh);
        
        if (mmh.getDestination() == null) {
          if (log.isInfoEnabled() && msgVerbose) {
            log.info("Recieved broadcast message event via down"); 
          }
          broadcastMessage(msg, true, true);
        } else if (mmh.getDestination().equals(local_addr.getAddress())) {
          if (log.isInfoEnabled() && msgVerbose) {
            log.info("Recieved message routed to self via down event");
          }
          final Message newMsg = msg.copy(true);
          newMsg.setDest(local_addr.getAddress());
          newMsg.setSrc(local_addr.getAddress());
          up_prot.up(new Event(Event.MSG, newMsg)); // just send back up the protocol stack
        } else {
          if (log.isInfoEnabled() && msgVerbose) {
            log.info("Routing message from down event to node: " + mmh.getDestination());
          }
          routeMsg(msg);
        }
        return null;
        //break;
      case Event.DISCONNECT:
        if (log.isInfoEnabled() && verbose) {
          log.info("Recieved disconnect event...disconnecting the node");
        }
        disconnected = true;
        conManager.abortAnyConnectionAttempt();
        Iterator<Address> it = meshModel.disconnectAllNodes().iterator();
        while (it.hasNext()) {
          Address node = it.next();
          if (log.isInfoEnabled() && highVerbose) {
            log.info("sending disconnect msg to node: " + node);
          }
          conManager.sendNodeDisconnectMsg(node);
        }
        if (log.isInfoEnabled() && verbose) {
          log.info("treeMesh node is now disconnected");
        }
        break;
      case Event.SET_LOCAL_ADDRESS:
        local_addr.setAddress((Address)event.getArg());
        if (log.isInfoEnabled() && verbose) {
          log.info("Setting local address: " + local_addr.getAddress());
        }
        break;
      default: 
        //log.warn("Got unhandled down event: " + event.toString());  // TODO - remove temp logging
        break;
    }
    return down_prot.down(event);
  }

  @Override
  public Object up(Event event) {
    switch (event.getType()) {
      case Event.MSG:
        if (disconnected) {
          break;
        }
        final Message msg = (Message)event.getArg();
        TreeMeshHeader hdr = (TreeMeshHeader)msg.getHeader(getId());
        
        if (hdr instanceof MeshHandshakeHeader) {
          conManager.newConnectionAttempt((HandshakeData)msg.getObject(), 
                                          msg.getSrc());
          return null;  // don't push this to the rest of the stack
        } else if (hdr instanceof PeerRecommendationHeader) {
          conManager.maybeHandlePeerRecommendation(msg.getSrc(), (PeerRecommendationData)msg.getObject());
          
          return null;  // don't push this to the rest of the stack
        } else if (hdr instanceof ConnectionClosedHeader) {
          ConnectionClosedHeader cch = (ConnectionClosedHeader)hdr;
          if (log.isInfoEnabled() && msgVerbose) {
            log.info("Got connection closed from node: " + msg.getSrc());
          }
          
          boolean resetConAttempt = conManager.resetConAttemptIfEqual(cch.getAttemptIdentifier());  // do this so we don't wait for it to timeout
          boolean droppedConnection = meshModel.connectionClosed(msg.getSrc());
          if (resetConAttempt && droppedConnection) {
            if (log.isWarnEnabled()) {
              log.warn("got a connectionClosed, " +
                       "which Reset a current connection attempt from an already connected node..." +
              		     "why were we trying to connect to an already connected node?");
            }
          }
          return null;  // don't push this to the application
        } else if (hdr instanceof MeshHeartbeatHeader) {
          MeshHeartbeatHeader mhh = (MeshHeartbeatHeader)hdr;
          if (rejectBroadcastMsg(mhh.getSender(), mhh.getMsgID())) { // verify this message has not arrived via a faster route first
            if (log.isInfoEnabled() && msgVerbose && highVerbose) {
              log.info("Recieved a heartbeat we have already seen via a different route: " + 
                       mhh.getSender() + " - " + mhh.getMsgID());
            }
            return null;
          }
          
          if (log.isInfoEnabled() && msgVerbose && highVerbose) {
            log.info("Recieved heartbeat message: " + mhh.getSender() + " - " + mhh.getMsgID());
          }
          if ( msgVerbose && log.isInfoEnabled() && 
              mhh.getHopsAddress().contains(local_addr.getAddress())) {
            log.info("heartbeat msg has visited this node! id: " + mhh.getMsgID());
          }
          // analyze the message for more mesh information
          meshModel.analyzeMessageHopRecord(mhh.getHopRecord(), local_addr.getAddress());
          broadcastMessage(msg, true, false);
          return null;  // don't push this to the rest of the stack
        } else if (hdr instanceof MeshMsgHeader) {
          final MeshMsgHeader mmh = (MeshMsgHeader)hdr;
          if (mmh.getDestination() == null) {
            if (rejectBroadcastMsg(mmh.getSender(), mmh.getMsgID())) { // verify this message has not arrived via a faster route first
              if (log.isInfoEnabled() && msgVerbose && highVerbose) {
                log.info("Recieved a message we have already seen via a different route: " + 
                         mmh.getSender() + " - " + mmh.getMsgID());
              }
              return null;
            }
            if (log.isInfoEnabled() && msgVerbose) {
              log.info("Got broadcast mesh message (application level) with id: " + mmh.getMsgID());
            }

            if (msgVerbose && log.isInfoEnabled() && mmh.getHopsAddress().contains(local_addr.getAddress())) {
              log.info("broadcast msg has visited this node! id: " + 
                       mmh.getMsgID() + ", destination: " + mmh.getDestination() + 
                       ", hopRecord: " + mmh.getHopsAddress());
            }
            // analyze the message for more mesh information
            meshModel.analyzeMessageHopRecord(mmh.getHopRecord(), local_addr.getAddress());
            
            broadcastMessage(msg, true, false);
            return null;  // message will be routed to application via broadcast
          } else if (mmh.getDestination().equals(local_addr.getAddress())) {
            if (log.isInfoEnabled() && msgVerbose) {
              log.info("Got mesh message destioned for us (application level) with id: " + mmh.getMsgID());
            }
            
            if (msgVerbose && log.isWarnEnabled() && 
                mmh.getHopsAddress().contains(local_addr.getAddress())) {
              log.warn("routed msg has visited this node! id: " + mmh.getMsgID() + 
                       ", destination: " + mmh.getDestination() + ", hopRecord: " + mmh.getHopsAddress());
            } else if (msgVerbose && highVerbose && mmh.getHopRecord().size() > 1) { // TODO - remove temp logging
              //System.out.println("---> recieved routed msg from node: " + mmh.getSender() + ", via route: " + mmh.getHopRecord().keySet());
            }
            // analyze the message for more mesh information
            meshModel.analyzeMessageHopRecord(mmh.getHopRecord(), local_addr.getAddress());
            // replace source with actual source, not just last node
            Message msgCopy = msg.copy(true);
            msgCopy.setSrc(mmh.getSender());
            // route to application
            return up_prot.up(new Event(Event.MSG, msgCopy));
          } else {
            if (log.isInfoEnabled() && msgVerbose) {
              log.info("Routing message from up event with id: " + mmh.getMsgID());
            }

            if (msgVerbose && log.isInfoEnabled() && 
                mmh.getHopsAddress().contains(local_addr.getAddress())) {
              log.info("routed msg has visited this node! id: " + mmh.getMsgID() + 
                       ", destination: " + mmh.getDestination() + ", hopRecord: " + mmh.getHopsAddress());
            }
            // analyze the message for more mesh information
            meshModel.analyzeMessageHopRecord(mmh.getHopRecord(), local_addr.getAddress());
            
            mmh.addHop(local_addr.getAddress(), Clock.nowInMilliseconds(), meshModel.getAvgHops(), 
                       meshModel.peerConnectionCount(), meshModel.leafConnectionCount());
            
            routeMsg(msg);
            return null;  // don't push this to the application
          }
        } else {
          if (log.isWarnEnabled()) {
            log.warn("Recieved a message with an unknown intention, ignoring msg with header: " + hdr);
          }
        }
        break;
      default: 
        log.warn("Got unhandled up event: " + event.toString());  // TODO - remove temp logging
        break;
    }
    //log.info("---> Sending to up_prot: " + event);  // TODO - remove temp logging
    return up_prot.up(event);
  }
  
  protected boolean rejectBroadcastMsg(Address sender, Long msgID) {
    synchronized (broadcastHistory) {
      if (msgWindowSize < 1) {
        if (log.isWarnEnabled()) {
          log.warn("msgWindowSize can not be less than 1, resetting to a valid value");
        }
        msgWindowSize = 1;
      }
      LinkedList<Long> senderHistory = broadcastHistory.remove(sender);
      if (senderHistory == null) {
        senderHistory = new LinkedList<Long>();
        senderHistory.add(msgID);
        broadcastHistory.put(sender, senderHistory);
        return false;
      } else {
        if (senderHistory.get(senderHistory.size() - 1) < msgID) {
          senderHistory.add(msgID);
          Iterator<Long> it = senderHistory.iterator();
          while (it.hasNext() && senderHistory.size() > msgWindowSize) {
            it.next();
            it.remove();
          }
          broadcastHistory.put(sender, senderHistory);
          return false;
        } else {
          if (senderHistory.get((int)0) >= msgID || senderHistory.contains(msgID)) {
            broadcastHistory.put(sender, senderHistory);
            return true;
          } else {
            long lastVal = -1;
            int index = -1;
            Iterator<Long> it = senderHistory.iterator();
            while (it.hasNext() && lastVal < msgID) {
              lastVal = it.next();
              index++;
            }
            if (log.isWarnEnabled() && verbose) {
              log.warn("Recieved out of order message from node: " + sender + 
                       " order degree: " + (senderHistory.size() - index));
            }
            senderHistory.add(index, msgID);
            broadcastHistory.put(sender, senderHistory);
            return false;
          }
        }
      }
    }
  }
  
  protected void routeMsg(final Message msg) {
    TreeMeshHeader hdr = (TreeMeshHeader)msg.getHeader(getId());
    final MeshMsgHeader mmh;
    if (hdr instanceof MeshMsgHeader) {
      mmh = (MeshMsgHeader)hdr;
    } else {
      throw new RuntimeException("Message does not contain a routable header");
    }
    
    final long msgSendStartTime = Clock.updateAndGetCurrTime();
    new Runnable() {
      public void run() {
        Address nextNode = meshModel.findMsgRouteNode(mmh.getDestination());
        if (nextNode != null) {
          if (log.isInfoEnabled() && msgVerbose) {
            log.info("Sending routed message id: " + mmh.getMsgID() + 
                     " to final destination " + mmh.getDestination() + 
                     " routed next to " + nextNode);
          }
          Message newMsg = msg.copy(true);
          newMsg.setDest(nextNode);
          newMsg.setSrc(local_addr.getAddress());
          down_prot.down(new Event(Event.MSG, newMsg));
        } else {  // retry later
          if (Clock.nowInMilliseconds() - msgSendStartTime < msgRoutingTimeout) {
            timer.schedule(this, 1000, TimeUnit.MILLISECONDS);
          } else if (log.isWarnEnabled()) {
            log.warn("Timeout sending msg to node: " + mmh.getDestination() + 
                     ", msgID: " + mmh.getMsgID() + ", message data: " + msg.getObject());
          }
        }
      }
    }.run();  // try to send and maybe schedule task to retry
  }
  
  public void sendHeartbeat() {
    if (local_addr.addressSet()) {
      Message msg = new Message(null, local_addr.getAddress(), null);
      if (runOOB) {
        msg.setFlag(Message.OOB);
      }
      MeshHeartbeatHeader mhh = new MeshHeartbeatHeader(local_addr.getAddress(), 
                                                        Clock.nowInMilliseconds(),
                                                        meshModel.getAvgHops(), 
                                                        meshModel.peerConnectionCount(), 
                                                        meshModel.leafConnectionCount());
      msg.putHeaderIfAbsent(getId(), mhh);

      if (log.isInfoEnabled() && highVerbose) {
        log.info("Sending heartbeat with id: " + mhh.getMsgID());
      }
      broadcastMessage(msg, false, true);
    } else if (log.isWarnEnabled()) {
      log.warn("Not sending heartbeat because local address has not been set yet");
    }
  }
  
  protected void broadcastMessage(Message msg, boolean addHop, boolean source) {
    TreeMeshHeader hdr = (TreeMeshHeader)msg.getHeader(getId());
    
    if (! (hdr instanceof MeshRoutedMsgHeader)) {
      if (log.isWarnEnabled()) {
        log.warn("Attempting to broadcast a message with an impropper header type");
      }
      return;
    }
    MeshRoutedMsgHeader mrmh = (MeshRoutedMsgHeader)hdr;
    if (mrmh.getDestination() != null) {
      if (log.isWarnEnabled()) {
        log.warn("Attempting to broadcast a message where the header destination is not null, " +
                 "discarding message: " + mrmh.getMsgID());
      }
      return;
    }
    
    if (! source && mrmh.getHopsAddress().contains(local_addr.getAddress())) { // msg has visited us, don't forward
      if (log.isInfoEnabled() && highVerbose && msgVerbose) {
        log.info("msg has already visited us, not sending: " + mrmh.getMsgID());
      }
      return;
    }
    
    if (addHop) {
      mrmh.addHop(local_addr.getAddress(), Clock.nowInMilliseconds(), meshModel.getAvgHops(), 
                  meshModel.peerConnectionCount(), meshModel.leafConnectionCount());
    }
    
    // forward the message on to all connected nodes
    Iterator<Address> it = meshModel.getConnectedNodes().iterator();
    while (it.hasNext()) {
      Address connectedNode = it.next();
      if (! mrmh.getHopsAddress().contains(connectedNode)) { // don't send to nodes this message has already visited
        if (log.isInfoEnabled() && highVerbose && msgVerbose) {
          log.info("sending broadcast message to node: " + connectedNode + " - " + mrmh.getMsgID());
        }
        Message newMsg = msg.copy(true);
        newMsg.setSrc(local_addr.getAddress());
        newMsg.setDest(connectedNode);
        down_prot.down(new Event(Event.MSG, newMsg));
      } else if (log.isInfoEnabled() && highVerbose && msgVerbose) {
        log.info("message has already visited node: " + connectedNode + " - " + mrmh.getMsgID());
      }
    }
    
    // lastly, if this is data the application may understand, forward up the stack
    if (mrmh instanceof MeshMsgHeader) {
      Address sender = mrmh.getSender();
      if (echoBroadcast || ! sender.equals(local_addr.getAddress())) {
        Message newMsg = msg.copy(true);
        newMsg.setDest(local_addr.getAddress());
        newMsg.setSrc(sender);
        up_prot.up(new Event(Event.MSG, newMsg));
      }
    }
  }
  
  public void sendNewView() {
    if (log.isInfoEnabled() && verbose) {
      log.info("Sending new view event");
    }
    nextViewID = (nextViewID % Long.MAX_VALUE) + 1;
    Event newViewEvent = new Event(Event.VIEW_CHANGE, 
                                   new MeshView(new ViewId(local_addr.getAddress(), nextViewID), 
                                                meshModel.getConnectedNodes(), meshModel.getRemoteNodes()));
    up_prot.up(newViewEvent);
    down_prot.down(newViewEvent);
  }
  
  protected class ConnectionManager {
    private long lastConnectionAttempt;
    private Address tryingToConnect;
    private final ReentrantLock connectionLock;
    private long attemptIdentifier;
    private final List<Address> attemptedNodes;
    private final List<Address> blacklistNodes; // this should currently only be used by human intervention
    private final Map<Address, Long> splitNodes;
    
    private ConnectionManager() {
      lastConnectionAttempt = 0;
      attemptIdentifier = -1; // we increment before we use this, so it is negative till we have our first connect attempt
      tryingToConnect = null;
      connectionLock = new ReentrantLock(true); // pass in true for fairness in locking
      attemptedNodes = new LinkedList<Address>();
      blacklistNodes = new LinkedList<Address>();
      splitNodes = new HashMap<Address, Long>();
    }
    
    public void maybeHandlePeerRecommendation(Address src, PeerRecommendationData recommendationData) {
      /* 
       * We will disconnect from the sender peer because 
       * we would have needed a responseHandShake in order 
       * to add to add them as a connected node, this this 
       * node should timeout from the connection table
       */
      boolean conAttemptReset = resetConAttemptIfEqual(recommendationData.getAttemptIdentifier());
      if (connectionLock.tryLock()) {
        try {
          if (log.isInfoEnabled() && verbose) {
            log.info("Got peer recommendation: " + recommendationData.getRecommendedNode() + " - attemptReset:" + 
                     conAttemptReset + " || (!tryingToConnect:" + 
                     (! tryingToConnect()) + " && (makePeer:" +
                     meshModel.makePeerConnection() + " || needToMerge:" + 
                     meshModel.needToMerge(peerConMax) + " || !canReachPeer:" + 
                     ! meshModel.canReachPeer(recommendationData.getRecommendedNode()) + "))");
          }
          if (! tryingToConnect() && (meshModel.makePeerConnection() || 
                                      meshModel.needToMerge(peerConMax) || 
                                      ! meshModel.canReachPeer(recommendationData.getRecommendedNode()))) {
            final ConnectionType conType;
            if (meshModel.makePeerConnection()) {
              conType = ConnectionType.ParentPeer;
            } else if (! meshModel.canReachPeer(recommendationData.getRecommendedNode())) {
              conType = ConnectionType.SplitPeer;
            } else if (meshModel.needToMerge(peerConMax)) {
              conType = ConnectionType.ParentPeer;
            } else {
              throw new RuntimeException("Got a peer recommendation and we don't know why");
            }
            if (meshModel.followPeerRecommendation(recommendationData.getRecommendedNode())) {
              attemptConnection(recommendationData.getRecommendedNode(), conType, 
                                "following peer recommendation from node: " + src);
            } else {
              if (log.isInfoEnabled() && verbose) {
                log.info("Ignoring peer recommendation ( " + recommendationData.getRecommendedNode() + " ) based on model response");
              }
            }
          } else {
            if (log.isInfoEnabled() && verbose) {
              log.info("Ignoring stale peer recommendation from node: " + src + " - " + 
                       (tryingToConnect == null) + "," + 
                       conAttemptReset + ", " + 
                       meshModel.makePeerConnection() + ", " + 
                       meshModel.needToMerge(peerConMax));
            }
          }
        } finally {
          connectionLock.unlock();
        }
      } else {
        // TODO - remove temp logging
        System.out.println("---> failed to get con_lock on peer recommendation to peer: " + 
                           recommendationData.getRecommendedNode() + 
                           " - needPeerCon:" + meshModel.makePeerConnection() + 
                           " - needToMerge:" + meshModel.needToMerge(peerConMax) + 
                           " - canReachPeer:" + meshModel.canReachPeer(recommendationData.getRecommendedNode()));
      }
    }

    protected boolean addBlacklist(Address node) {
      if (! blacklistNodes.contains(node)) {
        blacklistNodes.add(node);
        return true;
      }
      if (meshModel.connectionClosed(node)) {
        sendNodeDisconnectMsg(node);
      }
      return false;
    }
    
    protected boolean removeBlacklist(Address node) {
      return blacklistNodes.remove(node);
    }
    
    protected List<Address> clearBlacklist() {
      List<Address> removedNodes = Collections.unmodifiableList(blacklistNodes);
      blacklistNodes.clear();
      return removedNodes;
    }

    private void abortAnyConnectionAttempt() {
      Address sendAbortToNode = null;
      long attemptIdentifier = -1;
      connectionLock.lock();
      try {
        if (tryingToConnect()) {
          if (log.isInfoEnabled() && highVerbose) {
            log.info("Sending disconnect msg to node attempting to connect: " + tryingToConnect);
          }
          sendAbortToNode = tryingToConnect;
          attemptIdentifier = this.attemptIdentifier;
          tryingToConnect = null;
        }
      } finally {
        connectionLock.unlock();
      }
      if (sendAbortToNode != null) {
        sendNodeDisconnectMsg(sendAbortToNode, attemptIdentifier);
      }
    }
    
    // should only be used when disconnecting a node not in relation to a connection attempt
    protected void sendNodeDisconnectMsg(Address node) {
      sendNodeDisconnectMsg(node, -1);
    }
    
    protected void sendNodeDisconnectMsg(Address node, long connectionAttemptId) {
      if (node == null) {
        throw new RuntimeException("Can not send disconnect message as a broadcast");
      }
      Message disconnectMsg = new Message(node, local_addr.getAddress(), null);
      disconnectMsg.putHeaderIfAbsent(getId(), new ConnectionClosedHeader(connectionAttemptId));
      if (runOOB) {
        disconnectMsg.setFlag(Message.OOB);
      }
      down_prot.down(new Event(Event.MSG, disconnectMsg));
    }

    private boolean resetConAttemptIfEqual(long attemptIdentifier) {
      if (attemptIdentifier < 0) {
        return false; // this was not a valid attempt
      }
      
      connectionLock.lock();
      try {
        if (tryingToConnect() && this.attemptIdentifier == attemptIdentifier) {
          if (log.isInfoEnabled() && verbose) {
            log.info("Resetting connection attempt for node: " + tryingToConnect);
          }
          tryingToConnect = null;
          return true;
        } else {
          return false;
        }
      } finally {
        connectionLock.unlock();
      }
    }

    // TODO - maybe remove, I don't think this is a valid way to check attempts
    private boolean resetConAttemptIfEqual(Address src) {
      connectionLock.lock();
      try {
        if (tryingToConnect() && tryingToConnect.equals(src)) {
          if (log.isInfoEnabled() && verbose) {
            log.info("Resetting connection attempt for node: " + src);
          }
          tryingToConnect = null;
          return true;
        } else {
          return false;
        }
      } finally {
        connectionLock.unlock();
      }
    }
    
    private void newConnectionAttempt(final HandshakeData handshakeData, final Address nodeAddress) {
      if (nodeAddress.equals(local_addr.getAddress())) {  // TODO - this was probably from having a null PhysicalAddress, remove after we don't see it for a while
        log.warn("Got connection attempt from ourself: " + nodeAddress);
        meshModel.logConnectionState();
        new RuntimeException().printStackTrace();

        sendNodeDisconnectMsg(nodeAddress, handshakeData.getAttemptIdentifier());
      }
      
      if (handshakeData.isResponse() && tryingToConnect == null) {  // TODO - this was probably from having a null PhysicalAddress, remove after we don't see it for a while
        throw new RuntimeException("Got connection response, but not trying to connect to anyone: " + 
                                   nodeAddress + " - " + handshakeData.getConType());
      }
      
      if (disconnected) {
        if (log.isWarnEnabled()) {
          log.warn("Got connection attempt while node is in a disconnected state, ignoring request");
        }
        return;
      }
      
      // TODO - remove temp logging
      System.out.println(local_addr + " ----> got connection " + (handshakeData.isResponse() ? "response" : "attempt") + 
                         " from node: " + nodeAddress + ", for: " + handshakeData.getConType());
      
      if (handshakeData.getConType().isPeer()) {
        connectionLock.lock();
        try {
          int attemptCode = (handshakeData.isResponse() ? -1 : 
                                                          meshModel.acceptPeerConnection(handshakeData.getConType() == ConnectionType.SplitPeer ? handshakeData.getOtherMeshNodes().keySet() : null, // only pass in this information when relevant, split brain situations
                                                                                         tryingToConnect, 
                                                                                         local_addr.getAddress(),
                                                                                         (short) (tryingToConnect == null || 
                                                                                                  tryingToConnect.equals(nodeAddress) ? peerConMax : 
                                                                                                                                        peerConMax - 1), // if we are trying to connect to a different node, reduce the peerConMax by one so that we have space if they decide to accept us
                                                                                         peerConMax));
          if (handshakeData.isResponse() || attemptCode == MeshModel.ACCEPT_NEW_PEER_CONNECTION) {
            final boolean sendResponse = ! handshakeData.isResponse() && (tryingToConnect == null || 
                                                                          ! tryingToConnect.equals(nodeAddress));
            timer.schedule(new Runnable() {
              @Override
              public void run() {
                if (sendResponse) {
                  System.out.println(local_addr + " ---> responding to peer request with our information"); // TODO - remove temp logging
                  //respond with our information
                  Message msg = new Message(nodeAddress, local_addr.getAddress(), 
                                            meshModel.makeHandshakeData(handshakeData.getConType(), 
                                                                        handshakeData.getAttemptIdentifier(), 
                                                                        true));
                  if (runOOB) {
                    msg.setFlag(Message.OOB);
                  }
                  msg.putHeaderIfAbsent(getId(), new MeshHandshakeHeader());
                  down_prot.down(new Event(Event.MSG, msg));
                } else {
                  // TODO - remove temp logging
                  System.out.println(local_addr + " ---> not responding for peer node, isResponse: " + handshakeData.isResponse() + 
                                     " - tryingToConnect: " + tryingToConnect + " - " + nodeAddress);
                }
                newPeerConnectionAccepted(nodeAddress,
                                          // this is equally valid as just isResponse, just left here to understand the state of the system
                                          /*(isResponse &&
                                              (handshakeData.parentPeer() == null || 
                                               ! meshModel.canReachPeer(handshakeData.parentPeer()))) || 
                                            (handshakeData.parentPeer() != null && 
                                             ! meshModel.canReachPeer(handshakeData.parentPeer())),*/
                                          handshakeData.isResponse(),
                                          handshakeData.getSourceConDetails(),
                                          handshakeData.getOtherMeshNodes());
              }
            }, 0, TimeUnit.MILLISECONDS);
          } else if (attemptCode == MeshModel.PROVIDE_PEER_RECOMMENDATION) {  //provide recommendation
            timer.schedule(new Runnable() {
              @Override
              public void run() {
                List<Address> excludeList = new ArrayList<Address>(1);
                excludeList.add(nodeAddress); // to make sure we don't recommend this node to connect to itself
                Address peerRecommendation = meshModel.findConnectablePeer(excludeList, peerConMax, true);
                if (peerRecommendation != null) {
                  if (log.isInfoEnabled() && verbose) {
                    log.info("Providing recommendation based on attempted peer connection, recommendation: " + peerRecommendation);
                  }
                  Message msg = new Message(nodeAddress, local_addr.getAddress(), new PeerRecommendationData(handshakeData.getAttemptIdentifier(), 
                                                                                                             peerRecommendation));
                  msg.putHeaderIfAbsent(getId(), new PeerRecommendationHeader());
                  if (runOOB) {
                    msg.setFlag(Message.OOB);
                  }
                  down_prot.down(new Event(Event.MSG, msg));
        
                  /*
                   * This node should be disconnected because we provide a recommendation
                   * instead of a response handShake.  A response handShake should be
                   * the only reason they would add us as a connected node,
                   * so since nether node has them listed as a connected node, 
                   * they should not communicate and should timeout from the connection map
                   */
                } else {
                  if (log.isWarnEnabled()) {
                    log.warn("We were unable to find a peer recommendation for the node: " + nodeAddress);
                  }
                  /*
                   * This node should be disconnected because we don't send a response handShake.
                   * A response handShake should be the only reason they would add us as a 
                   * connected node, so since nether node has them listed as a connected node, 
                   * they should not communicate and should timeout from the connection map
                   */
                }
              }
            }, 0, TimeUnit.MILLISECONDS);
          } else if (attemptCode == MeshModel.DENY_CONNECTION) {
            timer.schedule(new Runnable() {
              @Override
              public void run() {
                if (log.isInfoEnabled()) {
                  log.info("Denying peer connection, sending denied response to node: " + nodeAddress);
                }
                sendNodeDisconnectMsg(nodeAddress, handshakeData.getAttemptIdentifier());
              }
            }, 0, TimeUnit.MILLISECONDS);
          } else {
            throw new RuntimeException("Model gave as an invalid response: " + attemptCode);
          }
        } finally {
          connectionLock.unlock();
        }
      } else if (handshakeData.getConType().isLeaf()) {
        connectionLock.lock();
        try {
          if (handshakeData.isResponse() ||   // only accept leaf connections if: it is a response, or we are not trying to connect, or we are trying to connect and this is the node, and we are in a good position to add a leaf
              ((tryingToConnect == null || tryingToConnect.equals(nodeAddress)) &&
                meshModel.acceptLeafConnection(peerConMax))) {  // TODO - i should make this logic more similar to how we accept peer connections
            if (log.isInfoEnabled() && verbose) {
              log.info("Accepting new leaf connection: " + nodeAddress);
            }
            final boolean sendResponse =  ! handshakeData.isResponse() && 
                                          (tryingToConnect == null || ! tryingToConnect.equals(nodeAddress));
            timer.schedule(new Runnable() {
              @Override
              public void run() {
                if (sendResponse) {
                  System.out.println(local_addr + " ---> responding to leaf request with our information"); // TODO - remove temp logging
                  //respond with our information
                  Message msg = new Message(nodeAddress, local_addr.getAddress(), 
                                            meshModel.makeHandshakeData(handshakeData.getConType(), 
                                                                        handshakeData.getAttemptIdentifier(), 
                                                                        true));
                  if (runOOB) {
                    msg.setFlag(Message.OOB);
                  }
                  msg.putHeaderIfAbsent(getId(), new MeshHandshakeHeader());
                  down_prot.down(new Event(Event.MSG, msg));
                } else {
                  // TODO - remove temp logging
                  System.out.println(local_addr + " ---> not responding for leaf node, isResponse: " + handshakeData.isResponse() + 
                                     " - tryingToConnect: " + tryingToConnect + " - " + nodeAddress);
                }
                newLeafConnectionAccepted(nodeAddress, handshakeData.getSourceConDetails());
              }
            }, 0, TimeUnit.MILLISECONDS);
          } else {
            timer.schedule(new Runnable() {
              @Override
              public void run() {
                if (log.isInfoEnabled() && verbose) {
                  log.info("Ignoring leaf connection attempt");
                }
                /*
                 * This node should be disconnected because we don't send a response handShake.
                 * A response handShake should be the only reason they would add us as a 
                 * connected node, so since nether node has them listed as a connected node, 
                 * they should not communicate and should timeout from the connection map
                 */
                 sendNodeDisconnectMsg(nodeAddress, handshakeData.getAttemptIdentifier());
              }
            }, 0, TimeUnit.MILLISECONDS);
          }
        } finally {
          connectionLock.unlock();
        }
      } else {
        // should not happen
        if (log.isWarnEnabled()) {
          log.warn("Got a connection attempt from a node with an unknown status: " + nodeAddress);
        }

        sendNodeDisconnectMsg(nodeAddress, handshakeData.getAttemptIdentifier());
      }
    }

    private void maybeMakePeerConnections() {
      meshModel.setInitialMembers(findInitialMembers());
      
      if (connectionLock.tryLock()) {
        try {
          if (! tryingToConnect()) {
            // update the splitNodes map with any now reachable nodes...we could maybe move this so it is done less frequently
            if (! splitNodes.isEmpty()) { // optimization
              List<Address> connectedNodes = meshModel.getConnectedNodes();
              List<Address> remoteNodes = meshModel.getRemoteNodes();
              Iterator<Address> it = splitNodes.keySet().iterator();
              while (it.hasNext()) {
                Address node = it.next();
                if (remoteNodes.contains(node) || connectedNodes.contains(node)) {
                  it.remove();
                }
              }
            }
            
            Address splitBrainNode = null;
            if (meshModel.makePeerConnection() || 
                (splitBrainNode = meshModel.findUnreachableNode(blacklistNodes)) != null) {
              final ConnectionType conType;
              final Address connectNode;
              if (splitBrainNode != null && 
                  ! meshModel.makePeerConnection()) { // we need to merge, follow split brain logic
                if (! meshModel.acceptPeerConnection(local_addr.getAddress(), peerConMax)) {  // but i am not the best one to merge it
                  Long noticedSplitTime = splitNodes.get(splitBrainNode);
                  if (noticedSplitTime == null || noticedSplitTime == 0) {
                    // create a peer recommendation for this node
                    List<Address> excludeList = new ArrayList<Address>(1);
                    excludeList.add(splitBrainNode); // to make sure we don't recommend this node to connect to itself
                    final Address peerRecommendation = meshModel.findConnectablePeer(excludeList, peerConMax, true);
                    if (peerRecommendation != null) {
                      if (log.isInfoEnabled() && verbose) {
                        log.info("Attempting to fix possible split brain, connectable node: " + splitBrainNode + 
                                 " is unreachable, suggesting peer node: " + peerRecommendation);
                      }
                      final Address finalSplitBrainNode = splitBrainNode;
                      timer.schedule(new Runnable() {
                        @Override
                        public void run() {
                          Message msg = new Message(finalSplitBrainNode, local_addr.getAddress(), new PeerRecommendationData(peerRecommendation));
                          msg.putHeaderIfAbsent(getId(), new PeerRecommendationHeader());
                          if (runOOB) {
                            msg.setFlag(Message.OOB);
                          }
                          down_prot.down(new Event(Event.MSG, msg));
                        }
                      }, 0, TimeUnit.MILLISECONDS);
                      
                      // record the time we provided the recommendation
                      splitNodes.put(splitBrainNode, new Long(Clock.updateAndGetCurrTime()));
                    } else {  // we need to merge and don't know who to recommend, we will retry once, but after that we will just wait on the timeout
                      if (log.isInfoEnabled() && verbose) {
                        log.info("Attempting to fix possible split brain, connectable node: " + splitBrainNode + " is unreachable, " +
                        		"but we are not the best peer node to find, and we don't have a good recommendation right now, will retry later");
                      }
                      /* If this was our first attempting, record we failed to find a recommendation...
                       * if it is our second attempt, pretend we made a recommendation, 
                       *    and hope that the real peer recommendation will notice this split peer...
                       *    otherwise we will just timeout and force ourself to do the dirty work */
                      splitNodes.put(splitBrainNode, new Long(noticedSplitTime == null ? 0 : Clock.updateAndGetCurrTime()));
                    }
                    return;
                  } else if (Clock.nowInMilliseconds() - noticedSplitTime < splitNodeRecommendationTimeout) {
                    // do nothing, leave the entry in the map
                    return;
                  } else {
                    if (log.isInfoEnabled() && verbose) {
                      log.info("Attempting to fix possible split brain, connectable node: " + splitBrainNode + 
                               " is unreachable, we are not the best node..." +
                               "but the node is still unrecable after timeout, so trying to connect directly");
                    }
                    splitNodes.remove(splitBrainNode);
                    if (meshModel.peerConnectionCount() < peerConMax) {
                      connectNode = splitBrainNode;
                      conType = ConnectionType.SplitPeer;
                    } else {
                      return;
                    }
                  }
                } else {  // we will just try to solve the split brain problem since we would have been the recommend node anyways
                  splitNodes.remove(splitBrainNode);  // verify they are not in this map
                  if (meshModel.peerConnectionCount() < peerConMax) {
                    if (log.isInfoEnabled() && verbose) {
                      log.info("Attempting to fix possible split brain, connectable node: " + splitBrainNode + 
                               " is unreachable, attempting to make peer connection to node");
                    }
                    connectNode = splitBrainNode;
                    conType = ConnectionType.SplitPeer;
                  } else {
                    if (log.isInfoEnabled() && verbose) {
                      log.info("Possible split brain, connectable node: " + splitBrainNode + 
                               " is unreachable, but too many peer connections to accept");
                    }
                    return;
                  }
                }
              } else {
                List<Address> excludeNodes;
                if (blacklistNodes.isEmpty()) { // optimized for memory allocation
                  excludeNodes = attemptedNodes;
                } else if (attemptedNodes.isEmpty()) {
                  excludeNodes = blacklistNodes;
                } else {
                  excludeNodes = new ArrayList<Address>(attemptedNodes.size() + blacklistNodes.size());
                  excludeNodes.addAll(attemptedNodes);
                  excludeNodes.addAll(blacklistNodes);
                }
                connectNode = meshModel.findConnectablePeer(excludeNodes, peerConMax, false);
                conType = ConnectionType.ParentPeer;
              }
              if (connectNode != null) {
                if (attemptConnection(connectNode, conType, "trying to add peer")) {
                  attemptedNodes.add(connectNode);
                }
              } else {
                attemptedNodes.clear(); // no result, so lets make sure we are not too strict
              }
            }
          }
        } finally {
          connectionLock.unlock();
        }
      } else {
        System.out.println("---> Failed to aquire lock for maybe making a peer connection"); // TODO - remove temp logging
      }
    }

    private void maybeMakeLeafConnections() {
      if (connectionLock.tryLock()) {
        try {
          if (! tryingToConnect() && meshModel.makeLeafConnection(peerConMax)) {
            List<Address> excludeNodes;
            if (blacklistNodes.isEmpty()) { // optimized for memory allocation
              excludeNodes = attemptedNodes;
            } else if (attemptedNodes.isEmpty()) {
              excludeNodes = blacklistNodes;
            } else {
              excludeNodes = new ArrayList<Address>(attemptedNodes.size() + blacklistNodes.size());
              excludeNodes.addAll(attemptedNodes);
              excludeNodes.addAll(blacklistNodes);
            }
            final Address connectNode = meshModel.findConnectableLeaf(excludeNodes, peerConMax);
            if (connectNode != null) {
              if (attemptConnection(connectNode, ConnectionType.Leaf, "trying to add leaf")) {
                attemptedNodes.add(connectNode);
              }
            } else {
              attemptedNodes.clear(); // no result, so lets make sure we are not too strict
            }
          }
        } finally {
          connectionLock.unlock();
        }
      } else {
        System.out.println("--> failed to get lock on maybe make leaf con");  // TODO - remove temp logging
      }
    }

    protected boolean tryingToConnect() {
      if (! connectionLock.isHeldByCurrentThread()) {
        throw new RuntimeException("Connection lock must be locked before calling this function");
      }
      
      if (tryingToConnect != null && 
          Clock.nowInMilliseconds() - lastConnectionAttempt > newConnectionTimeout) {
        if (log.isInfoEnabled() && verbose) {
          log.info("Timming out connection attempt to node: " + tryingToConnect);
        }
        tryingToConnect = null;
      }
        
      return tryingToConnect != null;
    }
    
    protected boolean attemptConnection(final Address nodeAddress, final ConnectionType conType, 
                                        String reason) {  // TODO - remove reason
      if (verbose && log.isInfoEnabled()) {
        log.info("Attempting to connect to node: " + nodeAddress + " with conType: " + conType + " ( " + reason + " )");
      }
      
      if (disconnected) {
        if (log.isWarnEnabled()) {
          log.warn("Ignoring connection attempt due to disconencted state");
        }
        return false;
      }
      
      if (connectionLock.tryLock()) {
        try {
          if (! tryingToConnect()) {
            if (conType.isPeer() && 
                meshModel.findUnreachableNode(blacklistNodes) == null && 
                ! meshModel.makePeerConnection() && 
                ! meshModel.needToMerge(peerConMax)) {  // TODO - remove temp safety check/logging
              System.out.println(local_addr + " ---> trying to find a peer connection when we should not: " + 
                                 meshModel.peerConnectionCount() + " ( " + reason + " )");
              new RuntimeException().printStackTrace();
            }
            lastConnectionAttempt = Clock.nowInMilliseconds();
            tryingToConnect = nodeAddress;
            
            timer.schedule(new Runnable() {
              @Override
              public void run() {
                // send message to node we are connecting to
                Message msg = new Message(nodeAddress, 
                                          local_addr.getAddress(), 
                                          meshModel.makeHandshakeData(conType, 
                                                                      ++attemptIdentifier, 
                                                                      false));
                if (runOOB) {
                  msg.setFlag(Message.OOB);
                }
                msg.putHeaderIfAbsent(getId(), new MeshHandshakeHeader());
                down_prot.down(new Event(Event.MSG, msg));
              }
            }, 0, TimeUnit.MILLISECONDS);
            
            return true;
          } else {
            if (verbose && log.isWarnEnabled()) { // TODO - remove temp logging
              log.warn("Ignorning attempt to make a connection, existing connection attempt happening");
            }
            return false;
          }
        } finally {
          connectionLock.unlock();
        }
      } else {
        System.out.println("---> Could not get lock for connection attempt to node: " + nodeAddress + " - " + conType + " - " + reason); // TODO - remove temp logging
        return false;
      }
    }
    
    @SuppressWarnings("unchecked")
    private List<PingData> findInitialMembers() {
      List<PingData> responses = (List<PingData>) down_prot.down(new Event(MEMBER_DISCOVERY_METHOD));
      if (responses != null) {
        for (Iterator<PingData> iter = responses.iterator(); iter.hasNext();) {
          Address address = iter.next().getAddress();
          if (address != null && local_addr.addressSet() && local_addr.getAddress().equals(address)) {
            iter.remove();
          } else if (address == null) {
            iter.remove();
          }
        }
      }
      return responses;
    }
    
    private void newPeerConnectionAccepted(Address newNode, 
                                           boolean canBeParent,
                                           NodeConnectionDetails nodCons, 
                                           Map<Address, NodeConnectionDetails> otherMeshNodes) {
      if (disconnected) {
        if (log.isWarnEnabled()) {
          log.warn("Ignoring connection accepted attempt due to disconencted state");
        }
        return;
      }
      connectionLock.lock();
      try {
        resetConAttemptIfEqual(newNode);
        meshModel.newPeerConnection(newNode, nodCons.getPeerConnections(), nodCons.getLeafConnections(), 
                                    canBeParent);
        
        int peerConCount = meshModel.peerConnectionCount();
        if (peerConCount > 1) { // if we are not a leaf node, we should not have leaf connections
          if (log.isInfoEnabled() && peerConCount == 2) {
            log.info("Disconnecting leaf connections, due to no longer being a leaf");
          }
          Iterator<Address> it = meshModel.disconnectAllLeafs().iterator();
          while (it.hasNext()) {
            final Address leafAddress = it.next();
            timer.schedule(new Runnable() {
              @Override
              public void run() {
                sendNodeDisconnectMsg(leafAddress);
              }
            }, 0, TimeUnit.MILLISECONDS);
          }
        }
        
        meshModel.updateModelFromHandshake(otherMeshNodes, local_addr.getAddress());
        
        if (log.isInfoEnabled() && verbose) {
          log.info("Established new peer connection to: " + newNode);
        }
      } finally {
        connectionLock.unlock();
      }
      sendNewView();
    }
    
    private void newLeafConnectionAccepted(Address newNode, 
                                           NodeConnectionDetails nodCons) {
      if (disconnected) {
        if (log.isWarnEnabled()) {
          log.warn("Ignoring connection accepted attempt due to disconencted state");
        }
        return;
      }

      connectionLock.lock();
      try {
        resetConAttemptIfEqual(newNode);
        meshModel.newLeafConnection(newNode, nodCons.getPeerConnections(), nodCons.getLeafConnections());
        
        if (log.isInfoEnabled() && verbose) {
          log.info("Established new leaf connection to: " + newNode);
        }
        
        attemptedNodes.clear();
      } finally {
        connectionLock.unlock();
      }
      sendNewView();
    }

    /* TODO - this logic needs to be redone....the problem is like this:
     * Assume we are the root node, and assume that one of our peers fails
     * That means that we no longer will detect we are the root node...and will then attempt to merge
     *  (this will create a non-tree based structure)
     * The problem is, I can't find a way for a node to realize that it is acting as a "degraded root"
     * Alternatively we don't want to eliminate nodes that we can already communicate too 
     *  (merging with nodes we can't communicate with is down elsewhere)
     *  
     * So instead I think the merge logic needs to change.  Possible solutions:
     * * if we previously were the root node, then we need to send a probe message.
     *   We use this probe message to better understand the tree structure and make
     *   sure that we are not causing loops with the peer connections
     * * If we were confident, we could make it so that once a root node, always a root node
     *   So once you were a root node, just assume you will never need to do any merging.
     *   Currently I am not confident that a root node would remain root always, but it seems
     *   very likely
     */
    public void mergeCheckAndMaybeMerge() {
      if (connectionLock.tryLock()) {
        try {
          if (! tryingToConnect() && meshModel.needToMerge(peerConMax)) {
            List<Address> excludeNodes;
            if (blacklistNodes.isEmpty()) { // optimized for memory allocation
              excludeNodes = attemptedNodes;
            } else if (attemptedNodes.isEmpty()) {
              excludeNodes = blacklistNodes;
            } else {
              excludeNodes = new ArrayList<Address>(attemptedNodes.size() + blacklistNodes.size());
              excludeNodes.addAll(attemptedNodes);
              excludeNodes.addAll(blacklistNodes);
            }
            Address connectNode = meshModel.findConnectablePeer(excludeNodes, peerConMax, false);
            if (connectNode != null) {
              if (verbose && log.isInfoEnabled()) {
                log.info("Attempting to merge with node: " + connectNode);
              }
              if (attemptConnection(connectNode, ConnectionType.ParentPeer, "trying to merge")) {
                attemptedNodes.add(connectNode);
              }
            } else {
              if (verbose && log.isInfoEnabled()) {
                log.info("Attempting to merge but could not find node to connect to, " +
                         "attempted nodes: " + attemptedNodes);
              }
              attemptedNodes.clear();
            }
          }
        } finally {
          connectionLock.unlock();
        }
      } else {
        log.warn("---> failed to get lock to do merge check!!");  // TODO - remove temp logging
      }
    }
  }
}
