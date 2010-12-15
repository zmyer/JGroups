package org.jgroups.protocols.jentMESH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.jentMESH.dataTypes.ConnectionType;
import org.jgroups.protocols.jentMESH.dataTypes.HandshakeData;
import org.jgroups.protocols.jentMESH.dataTypes.NodeConnectionDetails;
import org.jgroups.protocols.jentMESH.dataTypes.headers.MeshRoutedMsgHeader.MsgHop;
import org.jgroups.protocols.jentMESH.util.Clock;

public class MeshModel {
  // CONNECTION ATTEMPT RESPONSES
  public static final short ACCEPT_NEW_PEER_CONNECTION = 0;
  public static final short PROVIDE_PEER_RECOMMENDATION = 1;
  public static final short DENY_CONNECTION = 2;
  // END CONNECTION ATTEMPT RESPONSES
  
  // ROUTING MODELS
  public static final short NO_ROUTING_MODEL = 0;
  public static final short SIMPLE_ROUTING_MODEL = 1;
  public static final short ADVANCED_ROUTING_MODEL = 2;
  // END ROUTING MODELS
  
  public static short getModelVal(boolean useRoutingModel, boolean useAdvancedModel) {
    if (! useRoutingModel) {
      return NO_ROUTING_MODEL;
    } else if (useRoutingModel && ! useAdvancedModel) {
      return SIMPLE_ROUTING_MODEL;
    } else if (useRoutingModel && useAdvancedModel) {
      return ADVANCED_ROUTING_MODEL;
    }
    return NO_ROUTING_MODEL;
  }
  // END ROUTING MODELS
  
  // CLASS VARIABLES
  private final Log log = LogFactory.getLog(getClass());
  protected List<PingData> connectableNodes;
  protected Address parentPeer;
  protected long lastRootNodeTime = -1;
  protected final Map<Address, NodeDetails> connectedPeers;
  protected final Map<Address, NodeDetails> connectedLeafs;
  protected final Map<Address, RemoteNodeDetails> knownMeshNodes;
  protected final int routableModel;
  protected final long rootStickyTime;
  protected final short sampleSize;
  protected final boolean safetyChecks = true;  // checks which may increase load, but will help ensure sanity
  // END VARIABLES
  
  // CLASS STORAGE STRUCTURES
  protected class RemoteNodeDetails extends NodeDetails {
    protected long lastNodeTime = -1;
    protected Address stableNodeRoute;
    protected final Map<Address, List<Long>> possibleRoutes;
    protected final short[] hopsToUs;
    protected long count = 0;
    
    /*private RemoteNodeDetails(long lastNodeTime, short hops, 
     * short peerConnections, short leafConnections) {
      this(lastNodeTime, hops, peerConnections, leafConnections, Clock.updateAndGetCurrTime());
    }*/
    
    protected RemoteNodeDetails(long lastNodeTime, short hops, 
                                short peerConnections, short leafConnections, long time) {
      super(peerConnections, leafConnections, time);
      this.lastNodeTime = lastNodeTime;
      this.hopsToUs = new short[sampleSize];
      this.hopsToUs[0] = hops;
      count = 1;
      possibleRoutes = new HashMap<Address, List<Long>>();
    }
    
    protected RemoteNodeDetails(int lastNodeTime, short peerConnections, short leafConnections,
                                long nowInMilliseconds) {
      super(peerConnections, leafConnections, nowInMilliseconds);
      this.hopsToUs = new short[sampleSize];
      possibleRoutes = new HashMap<Address, List<Long>>();
    }

    protected void updateRecord(long lastNodeTime, Address fromNode, short hops) {
      if (this.lastNodeTime < lastNodeTime) {  // we only want to update from data that is newer, not older
        this.lastNodeTime = lastNodeTime;
        this.hopsToUs[(int)count % this.hopsToUs.length] = hops;
        count++;
        if (fromNode != null) {
          if (safetyChecks && routableModel == MeshModel.NO_ROUTING_MODEL) {
            if (log.isWarnEnabled()) {
              log.warn("fromNode not null, but not a routable model...ignoring");
              new RuntimeException().printStackTrace();
              return;
            }
          }
          if (routableModel == MeshModel.ADVANCED_ROUTING_MODEL) {
            updateRouteModel(fromNode);
          } else if (routableModel == MeshModel.SIMPLE_ROUTING_MODEL) {
            /*if (stableNodeRoute == null || ! stableNodeRoute.equals(fromNode)) {  // TODO - remove temp logging
              System.out.println("Updating route to use node: " + fromNode + " from " + stableNodeRoute);
            }*/
            stableNodeRoute = fromNode;
          }
        }
      }
    }
    
    protected void fixCycle() {
      synchronized (possibleRoutes) {
        possibleRoutes.remove(stableNodeRoute);
        stableNodeRoute = null;
        updateRouteModel(null);
      }
    }
    
    private void updateRouteModel(Address fromNode) {
      synchronized (possibleRoutes) {
        if (fromNode != null) {
          List<Long> routes = possibleRoutes.remove(fromNode);
          if (routes == null) {
            routes = new ArrayList<Long>(sampleSize + 1);
            routes.add(new Long(Clock.nowInMilliseconds()));
            possibleRoutes.put(fromNode, routes);
          } else {
            routes.add(new Long(Clock.nowInMilliseconds()));
            while (routes.size() > sampleSize) {
              routes.remove((int)0);
            }
            possibleRoutes.put(fromNode, routes);
          }
        }
        
        // if we had no previous route, and we are not repairing a cycle...then take this route whole heartedly
        if (stableNodeRoute == null && fromNode != null) {
          stableNodeRoute = fromNode;
        } else {
          // make sure all routes in possibleRoutes are connected nodes (so we don't have routes that point to no longer connected nodes)
          {
            Iterator<Address> it = possibleRoutes.keySet().iterator();
            while (it.hasNext()) {
              Address node = it.next();
              if (! (connectedPeers.containsKey(node) || connectedLeafs.containsKey(node))) {
                //System.out.println("---> removing possible route because this node is no longer connected: " + node + " - " + connectedPeers + " - " + connectedLeafs);
                it.remove();
              }
            }
          }
          
          long fastestUpdateTime = Long.MAX_VALUE;
          Address fastestRoute = null;
          Iterator<Entry<Address, List<Long>>> it = possibleRoutes.entrySet().iterator();
          while (it.hasNext()) {
            Entry<Address, List<Long>> entry = it.next();
            if (entry.getValue().size() < sampleSize) { // we need to have more date before we can recommend this route
              //System.out.println("Sample size not enough for node: " + entry.getKey() + " - " + entry.getValue().size());
              continue;
            }
            long routeTime = Clock.nowInMilliseconds() - entry.getValue().get((int)0);
            if (fastestUpdateTime > routeTime) {
              fastestUpdateTime = routeTime;
              fastestRoute = entry.getKey();
            }
          }
          
          if (fastestRoute != null) {
            /*if (stableNodeRoute == null || ! stableNodeRoute.equals(fastestRoute)) {
              System.out.println("Updating route to use node: " + fastestRoute + " from " + stableNodeRoute);
            }*/
            stableNodeRoute = fastestRoute;
          }
        }
      }
    }
    
    protected short getHopsToUs() {
      int entries;
      int avg = 0;
      
      if (count < this.hopsToUs.length) {
        entries = (int)count;
      } else {
        entries = hopsToUs.length;
      }

      int errorCt = 0;
      for (int i = 0; i < entries; i++) {
        if (hopsToUs[i] < 0) {
          errorCt++;
        } else {
          avg += hopsToUs[i];
        }
      }
      if (entries == 0 || entries - errorCt == 0) { // no valid entries to base this off of
        return -1;
      } else {
        return (short)(avg / (entries - errorCt));
      }
    }

    protected void reset() {  // currently unused
      for (int i = 0; i < hopsToUs.length; i++) {
        hopsToUs[i] = -1;
      }
      count = 0;
    }
  }
  
  protected class NodeDetails {
    protected final NodeConnectionDetails connections;
    protected long lastUpdateTime = -1;
    
    protected NodeDetails(short peerConnections, short leafConnections) {
      this(peerConnections, leafConnections, Clock.updateAndGetCurrTime());
    }
    
    protected NodeDetails(short peerConnections, short leafConnections, long time) {
      connections = new NodeConnectionDetails(peerConnections, leafConnections);
      lastUpdateTime = time;
    }
    
    protected NodeDetails(NodeConnectionDetails conDetails, long time) {
      lastUpdateTime = time;
      connections = conDetails;
    }
    
    protected NodeDetails getNodeDetailsCopy() {
      return new NodeDetails(connections, lastUpdateTime);
    }
    
    protected void updateConnectionsAndTime(short peerConnections, short leafConnections) {
      updateConnectionsAndTime(peerConnections, leafConnections, 
                               Clock.updateAndGetCurrTime());
    }
    
    protected void updateConnectionsAndTime(short peerConnections, short leafConnections, 
                                            long updateTime) {
      connections.setPeerConnections(peerConnections);
      connections.setLeafConnections(leafConnections);
      lastUpdateTime = updateTime;
    }
  }
  // END STORAGE STRUCTURES
 
  public MeshModel(short sampleSize, short routingLevel, long rootStickyTime) {
    if (sampleSize < 2) {
      throw new RuntimeException("SampleSize config value must be at least 2!");
    }
    
    parentPeer = null;
    this.sampleSize = sampleSize;
    this.routableModel = routingLevel;
    this.rootStickyTime = rootStickyTime;
    connectableNodes = new LinkedList<PingData>();
    connectedPeers = new HashMap<Address, NodeDetails>();
    connectedLeafs = new HashMap<Address, NodeDetails>();
    knownMeshNodes = new HashMap<Address, RemoteNodeDetails>();
  }

  public void updateModelFromHandshake(Map<Address, NodeConnectionDetails> meshNodes, 
                                       Address local_addr) {
    Iterator<Entry<Address, NodeConnectionDetails>> it = meshNodes.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Address, NodeConnectionDetails> nodeEntry = it.next();
      if (! nodeEntry.getKey().equals(local_addr) &&
          ! connectedPeers.containsKey(nodeEntry.getKey()) &&
          ! connectedLeafs.containsKey(nodeEntry.getKey()) &&
          ! knownMeshNodes.containsKey(nodeEntry.getKey())) {
        synchronized (knownMeshNodes) {
          knownMeshNodes.put(nodeEntry.getKey(), new RemoteNodeDetails(0, 
                                                                       nodeEntry.getValue().getPeerConnections(), 
                                                                       nodeEntry.getValue().getLeafConnections(), 
                                                                       Clock.nowInMilliseconds()));
        }
      }
    }
  }

  protected HashMap<Address, NodeConnectionDetails> prepareBasicMeshState() {
    HashMap<Address, NodeConnectionDetails> knownNodes = new HashMap<Address, NodeConnectionDetails>(connectedPeers.size() +
                                                                                                     connectedLeafs.size() +
                                                                                                     knownMeshNodes.size());
    {
      Iterator<Entry<Address, NodeDetails>> it = connectedPeers.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Address, NodeDetails> nodeEntry = it.next();
        knownNodes.put(nodeEntry.getKey(), nodeEntry.getValue().connections);
      }
      
      it = connectedLeafs.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Address, NodeDetails> nodeEntry = it.next();
        knownNodes.put(nodeEntry.getKey(), nodeEntry.getValue().connections);
      }
    }
    {
      Iterator<Entry<Address, RemoteNodeDetails>> it = knownMeshNodes.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Address, RemoteNodeDetails> nodeEntry = it.next();
        knownNodes.put(nodeEntry.getKey(), nodeEntry.getValue().connections);
      }
    }
    
    return knownNodes;
  }
  
  public HandshakeData makeHandshakeData(ConnectionType conType, 
                                         long attemptIdentifier, boolean isResponse) {
    Map<Address, NodeConnectionDetails> knownNodes = null;
    if (conType.isPeer()) { // set some additional data
      knownNodes = prepareBasicMeshState();
    }
    
    return new HandshakeData(conType, 
                             attemptIdentifier, 
                             isResponse, 
                             peerConnectionCount(), 
                             leafConnectionCount(), 
                             knownNodes);
  }

  // TODO - I am aware that the safety checks in this function are possible to occur...but I want to make sure that they are rare conditions, later these should be removed
  public void analyzeMessageHopRecord(List<MsgHop> hopRecord, Address local_addr) {
    //System.out.println("---> analyzing hop record: " + hopRecord.keySet());
    Address fromNode = null;
    short hopsAway = (short)hopRecord.size();
    
    if (hopRecord.isEmpty()) {
      if (log.isWarnEnabled()) {
        log.warn("Trying to analyze a message with no hops, skipping");
      }
      return;
    }
    
    if (routableModel != MeshModel.NO_ROUTING_MODEL) {
      // get the last entry in the hopRecord to find the fromNode
      Iterator<MsgHop> it = hopRecord.iterator();
      while (it.hasNext()) {  // loop till end
        fromNode = it.next().nodeAddress;
      }
    }
    
    Clock.update();

    Iterator<MsgHop> it = hopRecord.iterator();
    boolean warnedAboutSelfInRecord = false;
    while (it.hasNext()) {
      MsgHop msgHop = it.next();
      
      if (msgHop.nodeAddress.equals(local_addr)) {  //skip any records of ourself
        if (! warnedAboutSelfInRecord && safetyChecks) {  // TODO - remove temp logging
          //System.out.println"Skipping record of ourself in hop record (" + hopsAway + "): " + hopRecord);
          /*System.out.println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
          System.out.println("Routing map for node: " + local_addr);
          System.out.println("Connected nodes: " + getConnectedNodes());
          Iterator<Entry<Address, RemoteNodeDetails>> it2 = knownMeshNodes.entrySet().iterator();
          while (it2.hasNext()) {
            Entry<Address, RemoteNodeDetails> nodeEntry = it2.next();
            System.out.println(nodeEntry.getKey() + " routes via: " + nodeEntry.getValue().stableNodeRoute);
          }
          System.out.println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");*/
          warnedAboutSelfInRecord = true;
        }
        if (routableModel == MeshModel.ADVANCED_ROUTING_MODEL) {  // check if we have a loop
          Map<Address, Integer> results = new HashMap<Address, Integer>();
          Iterator<MsgHop> hopIt = hopRecord.iterator();
          while (hopIt.hasNext()) {
            Address node = hopIt.next().nodeAddress;
            Integer val = results.remove(node);
            if (val == null) {
              results.put(node, new Integer(1));
            } else {
              results.put(node, val + 1);
            }
            Iterator<Integer> testIt = results.values().iterator();
            int loopNodes = 0;
            while (testIt.hasNext()) {
              if (testIt.next() >= 2) {
                loopNodes++;
                if (loopNodes >= 2) { // we have cycled twice
                  break;
                }
              }
            }
            if (loopNodes >= 2) { // if we cycle at least twice, repair it
              if (log.isWarnEnabled()) {
                log.warn("Cycle detected from hop record, restting node routes possibly causing cycle");
              }
              Iterator<Entry<Address, Integer>> resultsIt = results.entrySet().iterator();
              while (resultsIt.hasNext()) {
                Entry<Address, Integer> resultEntry = resultsIt.next();
                if (resultEntry.getValue() >= 2) {
                  Iterator<RemoteNodeDetails> rndIt = knownMeshNodes.values().iterator();
                  while (rndIt.hasNext()) {
                    RemoteNodeDetails rnd = rndIt.next();
                    if (rnd.stableNodeRoute != null && 
                        rnd.stableNodeRoute.equals(resultEntry.getKey())) {
                      rnd.fixCycle();
                    }
                  }
                }
              }
              return; // don't analyze a msg that is just the same nodes over and over again
            }
          }
        }
        hopsAway--;
        continue;
      }
      
      if (hopsAway == 1) {   // since this is only one hop away, we should be directly connected to it
        //System.out.println("---> analyzing node that is directly connected to us: " + nodeEntry.getKey()); // TODO - remove logging
        if (log.isWarnEnabled() && safetyChecks) {
          if (knownMeshNodes.containsKey(msgHop.nodeAddress)) {
            log.warn("MMER:amhr1 - This node is one hop away, and maintained in knownMeshNodes: " + msgHop.nodeAddress + 
                     " - " + hopRecord + " - " + connectedPeers.keySet() + " - " + connectedLeafs.keySet() + 
                     " - " + knownMeshNodes.keySet());
            continue;
          }
        }
        
        NodeDetails nd = connectedPeers.get(msgHop.nodeAddress);
        if (nd != null) { // update connected peer
          nd.updateConnectionsAndTime(msgHop.peerConnections, msgHop.leafConnections, 
                                      Clock.nowInMilliseconds());
        } else {
          nd = connectedLeafs.get(msgHop.nodeAddress);
          if (nd != null) { // update connected leaf
            nd.updateConnectionsAndTime(msgHop.peerConnections, msgHop.leafConnections, 
                                        Clock.nowInMilliseconds());
          } else if (log.isWarnEnabled()) { // This can happen when a node has received our connection request, but we have not received their response yet
            log.warn("MMER:amhr2 - Node is supposedly one hop away, but we are not directly connected to them, " +
            		"discarding hop detail for node: " + msgHop.nodeAddress + " - " + hopRecord + 
            		" - " + connectedPeers.keySet() + " - " + connectedLeafs.keySet() + " - " + knownMeshNodes.keySet());
            continue;
          }
        }
      } else {  // this should be a distant node, but even if it is not, we wont want to update locally connected nodes via old information
        //System.out.println("---> analyzing a remote node: " + nodeEntry.getKey()); // TODO - remove logging
        RemoteNodeDetails rnd = knownMeshNodes.get(msgHop.nodeAddress);
        if (rnd != null) {
          //System.out.println("---> this is a remote node we already knew about, updating record"); // TODO - remove logging
          // Update already known mesh node
          rnd.updateRecord(msgHop.nodeTime, fromNode, hopsAway);
          rnd.updateConnectionsAndTime(msgHop.peerConnections, msgHop.leafConnections, 
                                       Clock.nowInMilliseconds());
        } else {
          // we have to make sure we are not already connected to this node before we just add an entry for them
          if (connectedPeers.containsKey(msgHop.nodeAddress) || connectedLeafs.containsKey(msgHop.nodeAddress)) {
            continue;
          }
          
          //System.out.println("----> this is a new remote node, adding new record for node: " + nodeEntry.getKey()); // TODO - remove logging
          synchronized (knownMeshNodes) {
            knownMeshNodes.put(msgHop.nodeAddress, new RemoteNodeDetails(msgHop.nodeTime,
                                                                         hopsAway, 
                                                                         msgHop.peerConnections, 
                                                                         msgHop.leafConnections, 
                                                                         Clock.nowInMilliseconds()));
          }
        }
      }
      hopsAway--;
    }
  }
  
  public boolean isModelStable() {
    synchronized (knownMeshNodes) {
      Iterator<RemoteNodeDetails> it = knownMeshNodes.values().iterator();
      while (it.hasNext()) {
        if (it.next().count < sampleSize) {
          System.out.println("---> mesh is NOT stable");  // TODO - remove temp logging
          return false;
        }
      }
      System.out.println("---> mesh is stable with node qty: " + knownMeshNodes.size());  // TODO - remove temp logging
      return true;
    }
  }
  
  public boolean makePeerConnection() {
    if (connectedPeers.isEmpty()) {
      return true;
    } else {
      return false;
    }
  }
  
  public boolean makeLeafConnection(short peerConMax) {
    return connectedPeers.size() == 1 && connectedLeafs.size() < (peerConMax - 1);
  }
  
  public short peerConnectionCount() {
    return (short)connectedPeers.size();
  }
  
  public short leafConnectionCount() {
    return (short)connectedLeafs.size();
  }
  
  public short getAvgHops() {
    if (knownMeshNodes.size() == 0) { return 0; } // prevent division by zero error
    
    short result = 0;
    int badHopCounts = 0;
    synchronized (knownMeshNodes) {
      Iterator<RemoteNodeDetails> it = knownMeshNodes.values().iterator();
      while (it.hasNext()) {
        RemoteNodeDetails rnd = it.next();
        short hops = rnd.getHopsToUs();
        if (hops > 0) {
          result += hops;
        } else {
          badHopCounts++;
        }
      }
      if ((knownMeshNodes.size() - badHopCounts) == 0) {
        //System.err.println(parentID + " - no valid hop counts to return an average, returning -1");
        result = -1;
      } else {
        result = (short)Math.ceil((double)result / (knownMeshNodes.size() - badHopCounts));
      }
    }
    
    return result;
  }

  public void setInitialMembers(List<PingData> initialMembers) {
    if (initialMembers != null) {
      /*{ // TODO - remove temp logging
        List<Address> oldMembers = new ArrayList<Address>(connectableNodes.size());
        List<Address> newMembers = new ArrayList<Address>(initialMembers.size());
        Iterator<PingData> it = connectableNodes.iterator();
        while (it.hasNext()) {
          oldMembers.add(it.next().getAddress());
        }
        it = initialMembers.iterator();
        while (it.hasNext()) {
          newMembers.add(it.next().getAddress());
        }
        if (! (oldMembers.containsAll(newMembers) && newMembers.containsAll(oldMembers))) {
          System.out.println("---> SETTING INITIAL MEMBERS - old member set: " + oldMembers + ", new set: " + newMembers);
        }
      }*/
      connectableNodes = initialMembers;
    }
  }

  public boolean acceptPeerConnection(Address address, short peerConMax) {
    return acceptPeerConnection(null, null, address, peerConMax, peerConMax) == ACCEPT_NEW_PEER_CONNECTION;
  }

  public int acceptPeerConnection(Set<Address> reachableNodes, Address attemptingToConnect, 
                                  Address local_addr, short peerConMax, short globalPeerConMax) {
    if (peerConnectionCount() >= peerConMax) { // quick and easy check, do we have space for this connection
      return PROVIDE_PEER_RECOMMENDATION;
    }
    
    // this is to make sure that only the root node does not have any parent peers
    if (parentPeer != null && 
        connectedPeers.get(parentPeer).connections.getPeerConnections() < globalPeerConMax) {
      System.out.println("******** TRYING TO PRESERVE ROOT STRUCTURE"); // TODO remove temp logging
      return PROVIDE_PEER_RECOMMENDATION;
    }
    
    if (reachableNodes != null) {
      // don't accept this connection as a peer if the remote node has overlap in the mesh structure
      if (attemptingToConnect != null && reachableNodes.contains(attemptingToConnect)) {
        if (log.isInfoEnabled()) {
          log.info("Denying incoming peer connection due to overlap a peer we are already trying to connect to: " + 
                   attemptingToConnect);
        }
        return DENY_CONNECTION;
      }
      Iterator<Address> it = reachableNodes.iterator();
      while (it.hasNext()) {
        Address remoteKnownNode = it.next();
        if (connectedPeers.containsKey(remoteKnownNode)) {
          if (log.isInfoEnabled()) {
            log.info("Denying incoming peer connection due to overlap with a connected peer: " + 
                     connectedPeers.keySet() + " - " + reachableNodes);
          }
          return DENY_CONNECTION;
        } else if (connectedLeafs.containsKey(remoteKnownNode)) {
          if (log.isInfoEnabled()) {
            log.info("Denying incoming peer connection due to overlap with a connected leaf: " + 
                     connectedLeafs.keySet() + " - " + reachableNodes);
          }
          return DENY_CONNECTION;
        } else if (knownMeshNodes.containsKey(remoteKnownNode)) {
          if (log.isInfoEnabled()) {
            log.info("Denying incoming peer connection due to overlap with a remote node: " + 
                     knownMeshNodes.keySet() + " - " + reachableNodes);
          }
          return DENY_CONNECTION;
        } else if (local_addr.equals(remoteKnownNode)) {
          if (log.isInfoEnabled()) {
            log.info("Denying incoming peer connection due to overlap with ourself: " + 
                     local_addr + " - " + reachableNodes);
          }
          return DENY_CONNECTION;
        }
      }
    }
    
    Map<Address, NodeDetails> allNodes = new HashMap<Address, NodeDetails>(1 + 
                                                                           knownMeshNodes.size() + 
                                                                           connectedPeers.size() + 
                                                                           connectedLeafs.size());
    allNodes.putAll(knownMeshNodes);
    allNodes.putAll(connectedPeers);
    allNodes.putAll(connectedLeafs);
    allNodes.put(local_addr, new NodeDetails(peerConnectionCount(), leafConnectionCount(), 
                                             Clock.nowInMilliseconds()));
    
    if (peerConnectRecommendationList(allNodes, peerConMax).contains(local_addr)) {
      return ACCEPT_NEW_PEER_CONNECTION;
    } else {
      return PROVIDE_PEER_RECOMMENDATION;
    }
  }
  
  public Address findUnreachableNode(List<Address> blacklistNodes) {
    if (blacklistNodes == null) {  // fix possible NPE
      blacklistNodes = new ArrayList<Address>();
    }
    if (parentPeer != null) { // only look for unreachable nodes if we have a parent peer
      Iterator<PingData> it = connectableNodes.iterator();
      while (it.hasNext()) {
        Address nodeAddress = it.next().getAddress();
        if (! blacklistNodes.contains(nodeAddress) &&
            ! (connectedPeers.containsKey(nodeAddress) 
            || connectedLeafs.containsKey(nodeAddress) 
            || knownMeshNodes.containsKey(nodeAddress))) {
          return nodeAddress;
        }
      }
    }
    return null;
  }
  
  public Address findConnectablePeer(List<Address> excludeNodes, short peerConMax, boolean peerRecommendation) {
    if (parentPeer != null && // this is to help preserve root structure, as well as being a very quick test 
        connectedPeers.get(parentPeer).connections.getPeerConnections() < peerConMax) {
      return parentPeer;
    }
    
    Map<Address, NodeDetails> meshNodes = new HashMap<Address, NodeDetails>(knownMeshNodes.size() + 
                                                                           (peerRecommendation ? connectedPeers.size() : 0) + 
                                                                           connectedLeafs.size());
    meshNodes.putAll(knownMeshNodes);
    if (peerRecommendation) {
      meshNodes.putAll(connectedPeers);
    }
    meshNodes.putAll(connectedLeafs);
    
    if (excludeNodes != null) {
      Iterator<Address> it = meshNodes.keySet().iterator();
      while (it.hasNext()) {
        Address address = it.next();
        Iterator<Address> it2 = excludeNodes.iterator();
        while (it2.hasNext()) {
          Address excludeAddress = it2.next();
          if (address.equals(excludeAddress)) {
            it.remove();
            break;
          }
        }
      }
    }
    
    List<Address> maxPeers = peerConnectRecommendationList(meshNodes, peerConMax);
    int maxPeersSize = maxPeers.size();
    if (maxPeersSize > 1) {
      return maxPeers.get((int)Math.round(Math.random() * (maxPeersSize - 1)));
    } else if (maxPeersSize > 0) {
      return maxPeers.get(0);
    } else {
      // Possibly ourselves trying to connect for the first time, or we were asked for a recommendation before we have learned anything about the mesh state
      int connectableNodesSize = connectableNodes.size();
      List<PingData> fooConnectableNodes = new ArrayList<PingData>(connectableNodesSize);
      fooConnectableNodes.addAll(connectableNodes);
      if (! peerRecommendation) {
        Iterator<PingData> it = fooConnectableNodes.iterator();
        while (it.hasNext()) {
          Address connectableNode = it.next().getAddress();
          Iterator<Address> it2 = connectedPeers.keySet().iterator();
          while (it2.hasNext()) {
            if (it2.next().equals(connectableNode)) {
              it.remove();
              break;
            }
          }
        }
      }
      connectableNodesSize = fooConnectableNodes.size();
      if (connectableNodesSize > 0) {
        return fooConnectableNodes.get((int)Math.round(Math.random() * 
                                                       (connectableNodesSize - 1))).getAddress();
      }
      return null;
    }
  }
  
  protected List<Address> peerConnectRecommendationList(Map<Address, NodeDetails> nodes, short peerConMax) {
    Iterator<Entry<Address, NodeDetails>> it = nodes.entrySet().iterator();
    List<Address> maxPeer = new LinkedList<Address>();
    short maxCon = Short.MIN_VALUE;
    while (it.hasNext()) {
      Entry<Address, NodeDetails> entry = it.next();
      if (entry.getValue().connections.isSet() &&
          entry.getValue().connections.getPeerConnections() < peerConMax && 
          entry.getValue().connections.getPeerConnections() > maxCon) {
        maxPeer.clear();
        maxPeer.add(entry.getKey());
        maxCon = entry.getValue().connections.getPeerConnections();
      } else if (entry.getValue().connections.isSet() &&
                 entry.getValue().connections.getPeerConnections() == maxCon) {
        maxPeer.add(entry.getKey());
      }
    }
    return maxPeer;
  }
  
  // find the most distant leaf node to connect to
  public Address findConnectableLeaf(List<Address> excludeNodes, short peerConMax) {
    List<Address> result = new LinkedList<Address>();
    int maxHops = -1; // start at negative 1 so that we will have a result even if state is cleared
    
    synchronized (knownMeshNodes) {
      Iterator<Entry<Address, RemoteNodeDetails>> it = knownMeshNodes.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Address, RemoteNodeDetails> nodeEntry = it.next();
        if (! excludeNodes.contains(nodeEntry.getKey()) &&             // this node is not on our exclusions list
            nodeEntry.getValue().count >= sampleSize &&                // verify this node is stable
            nodeEntry.getValue().connections.isSet() &&                // verify we have information about this nodes connections
            nodeEntry.getValue().connections.getPeerConnections() == 1 &&   // peerConnections must == 1 because it must be a leaf node
            nodeEntry.getValue().connections.getLeafConnections() < peerConMax - 1) {  // does this peer have a free leaf slot?
          int hops = nodeEntry.getValue().getHopsToUs();
          if (hops > maxHops) {
            maxHops = hops;
            result.clear();
            result.add(nodeEntry.getKey());
          } else if (hops == maxHops) {
            result.add(nodeEntry.getKey());
          }
        }
      }
    }
    
    Address finalResult = null;
    int resultSize = result.size();
    if (resultSize > 1) {
      finalResult = result.get((int)Math.round(Math.random() * (resultSize - 1)));
    } else if (resultSize > 0) {
      finalResult = result.get(0);
    }
    
    return finalResult;
  }
  
  public boolean needToMerge(int peerConMax) {
    // TODO - remove temp logging
    int conPeerSize = connectedPeers.size();
    int expectedConCount = Math.min(peerConMax, 
                                    Math.max(connectableNodes.size(), 
                                             knownMeshNodes.size() + 
                                               connectedPeers.size() + 
                                               connectedLeafs.size()));
    if (parentPeer == null && conPeerSize > 0 && conPeerSize < expectedConCount) {
      System.out.println("---> requesting merge, connected peers/leafs: " + connectedPeers.keySet() + " - " + connectedLeafs.keySet());
    } else if (parentPeer == null && conPeerSize > 0) {
      System.out.println("---> not requesting merge...ROOT NODE!");
    }
    // TODO - end of temp logging
    
    boolean cached;
    if (cached = (Clock.nowInMilliseconds() - lastRootNodeTime < rootStickyTime) || // if we were the root node within a time check
          (parentPeer == null &&                                                    // otherwise check if we could still be the root node
            connectedPeers.size() > 0 && 
            connectedPeers.size() >= Math.min(/*Math.min(peerConMax, 
                                                       getMostPeerCons(-1)),  // TODO - alternatively maybe we should just use getMostPeerCons and not peerConMax?*/  // this was commented out because getMostPeerCons would only solve some of the merge cases
                                              peerConMax,
                                              Math.max(connectableNodes.size(), 
                                                       knownMeshNodes.size() + 
                                                         connectedPeers.size() + 
                                                         connectedLeafs.size())))) {
      if (! cached) {
        lastRootNodeTime = Clock.nowInMilliseconds();
      }
      return false;
    } else {  // we are not the root node, but do we need to merge?
      lastRootNodeTime = -1;
      return parentPeer == null && conPeerSize > 0 && conPeerSize < expectedConCount;  // if we are not connected to any peers, then TREEMESH should handle this, and not FD/merge
    }
  }
  
  /*public boolean isRootNode() {
    if (safetyChecks && 
        ((parentPeer == null && lastRootNodeTime >= 0) || 
         (parentPeer != null && lastRootNodeTime < 0))) {
      throw new RuntimeException("Confused state about root node");
    }
    return lastRootNodeTime < 0 && parentPeer == null;
  }*/
  
  /*private short getMostPeerCons(int peerConMax) {
    short maxPeerCon = -1;
    {
      Iterator<NodeDetails> it = connectedPeers.values().iterator();
      while (it.hasNext()) {
        short nodePeerCon = it.next().connections.getPeerConnections();
        if (nodePeerCon > maxPeerCon) {
          maxPeerCon = nodePeerCon;
          if (maxPeerCon == peerConMax) { // possible optimization, pass in -1 to avoid
            return maxPeerCon;
          }
        }
      }
      it = connectedLeafs.values().iterator();
      while (it.hasNext()) {
        short nodePeerCon = it.next().connections.getPeerConnections();
        if (nodePeerCon > maxPeerCon) {
          maxPeerCon = nodePeerCon;
          if (maxPeerCon == peerConMax) { // possible optimization, pass in -1 to avoid
            return maxPeerCon;
          }
        }
      }
    }
    {
      Iterator<RemoteNodeDetails> it = knownMeshNodes.values().iterator();
      while (it.hasNext()) {
        short nodePeerCon = it.next().connections.getPeerConnections();
        if (nodePeerCon > maxPeerCon) {
          maxPeerCon = nodePeerCon;
          if (maxPeerCon == peerConMax) { // possible optimization, pass in -1 to avoid
            return maxPeerCon;
          }
        }
      }
    }
    return maxPeerCon;
  }*/

  public void newPeerConnection(Address newPeer, short peerConnections, short leafConnections, 
                                boolean canBeParent) {
    if (safetyChecks && connectedPeers.containsKey(newPeer)) {
      if (log.isWarnEnabled()) {
        log.warn("This is already a connected peer, ignoring duplicate connection");
        System.out.println("----> This is already a connected peer, ignoring duplicate connection");
      }
      return;
    }
    
    if (canBeParent && (parentPeer == null || peerConnectionCount() == 0)) {
      if (parentPeer != null && log.isWarnEnabled()) {
        log.warn("New peer connection when another has already been set, parentPeer listed as active peer: " + 
                 connectedPeers.containsKey(parentPeer));
      }
      if (log.isInfoEnabled()) {
        log.info("Setting new parent peer to be node: " + newPeer);
      }
      parentPeer = newPeer;
    }
    
    synchronized (knownMeshNodes) {
      RemoteNodeDetails rnd = knownMeshNodes.remove(newPeer);
      synchronized (connectedPeers) {
        if (rnd != null) {
          connectedPeers.put(newPeer, rnd.getNodeDetailsCopy());
        } else {
          NodeDetails nd = connectedLeafs.remove(newPeer);  // check if this used to be a leaf
          if (nd != null) {
            connectedPeers.put(newPeer, nd);
          } else {
            connectedPeers.put(newPeer, new NodeDetails(peerConnections, leafConnections));
          }
        }
      }
    }
  }

  public void newLeafConnection(Address newLeaf, short peerConnections, short leafConnections) {
    if (safetyChecks && connectedLeafs.containsKey(newLeaf)) {
      if (log.isWarnEnabled()) {
        log.warn("This is already a connected leaf, ignoring duplicate connection");
      }
      return;
    }
    
    synchronized (knownMeshNodes) {
      RemoteNodeDetails rnd = knownMeshNodes.remove(newLeaf);
      synchronized (connectedLeafs) {
        if (rnd != null) {
          connectedLeafs.put(newLeaf, rnd.getNodeDetailsCopy());
        } else {
          connectedLeafs.put(newLeaf, new NodeDetails(peerConnections, leafConnections));
        }
      }
    }
  }
  
  public List<Address> getConnectedPeers() {
    return Collections.unmodifiableList(new ArrayList<Address>(connectedPeers.keySet()));
  }
  
  public List<Address> getConnectedLeafs() {
    return Collections.unmodifiableList(new ArrayList<Address>(connectedLeafs.keySet()));
  }
  
  public List<Address> getConnectedNodes() {
    List<Address> result = new ArrayList<Address>(connectedLeafs.size() + connectedPeers.size());
    result.addAll(connectedPeers.keySet());
    result.addAll(connectedLeafs.keySet());
    return Collections.unmodifiableList(result);
  }
  
  public List<Address> getRemoteNodes() {
    List<Address> result = new ArrayList<Address>(knownMeshNodes.size());
    result.addAll(knownMeshNodes.keySet());
    return Collections.unmodifiableList(result);
  }
  
  public List<Address> getReachableNodes() {
    List<Address> remoteNodes = getRemoteNodes();
    List<Address> connectedNodes = getConnectedNodes();
    List<Address> result = new ArrayList<Address>(remoteNodes.size() + connectedNodes.size());
    result.addAll(connectedNodes);
    result.addAll(remoteNodes);
    return Collections.unmodifiableList(result);
  }
  
  public List<Address> disconnectAllLeafs() {
    List<Address> removedNodes = Collections.unmodifiableList(new ArrayList<Address>(connectedLeafs.keySet()));
    /*
     * By removing all leafs from this list, we wont communicate with them any longer
     * The connection map should later timeout these dead connections
     */
    connectedLeafs.clear();
    return removedNodes;
  }

  public boolean acceptLeafConnection(short peerConMax) {
    return peerConnectionCount() == 1 && leafConnectionCount() < peerConMax - 1;
  }

  public boolean timeoutDeadNodes(int timeout) {
    boolean removedNode = false;
    Clock.update();
    
    synchronized (connectedPeers) { 
      Iterator<Entry<Address, NodeDetails>> it = connectedPeers.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Address, NodeDetails> nodeEntry = it.next();
        if (Clock.nowInMilliseconds() - nodeEntry.getValue().lastUpdateTime > timeout) {
          if (parentPeer != null && parentPeer.equals(nodeEntry.getKey())) {
            System.out.println("---> timming out PARENT peer node: " + nodeEntry.getKey() + 
                               " delay: " + (Clock.nowInMilliseconds() - nodeEntry.getValue().lastUpdateTime));
            parentPeer = null;
          } else {
            System.out.println("---> timing out peer node: " + nodeEntry.getKey() + 
                               " delay: " + (Clock.nowInMilliseconds() - nodeEntry.getValue().lastUpdateTime));
          }
          removedNode = true;
          it.remove();
        }
      }
    }
    
    synchronized (connectedLeafs) {
      Iterator<Entry<Address, NodeDetails>> it = connectedLeafs.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Address, NodeDetails> entry = it.next();	// TODO - this does not need to be stored or an entry except for logging
        if (Clock.nowInMilliseconds() - entry.getValue().lastUpdateTime > timeout) {
          System.out.println("---> timing out leaf node: " + entry.getKey() + 
                             " delay: " + (Clock.nowInMilliseconds() - entry.getValue().lastUpdateTime));
          removedNode = true;
          it.remove();
        }
      }
    }
    
    synchronized (knownMeshNodes) {
      Iterator<Entry<Address, RemoteNodeDetails>> it = knownMeshNodes.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Address, RemoteNodeDetails> nodeEntry = it.next();  // TODO - this does not need to be stored or an entry except for logging
        if (Clock.nowInMilliseconds() - nodeEntry.getValue().lastUpdateTime > timeout) {
          System.out.println("---> timing out remote node: " + nodeEntry.getKey());
          removedNode = true;
          it.remove();
        }
      }
    }
    
    if (removedNode) {	// TODO - remove temp logging
      System.out.println("----> timeStats: " + Clock.getStats());
    }
    
    return removedNode;
  }

  public Address findMsgRouteNode(Address destination) {
    // first check connected nodes
    {
      synchronized (connectedPeers) {
        Iterator<Address> it = connectedPeers.keySet().iterator();
        while (it.hasNext()) {
          if (it.next().equals(destination)) {
            return destination;
          }
        }
      }
      
      synchronized (connectedLeafs) {
        Iterator<Address>  it = connectedLeafs.keySet().iterator();
        while (it.hasNext()) {
          if (it.next().equals(destination)) {
            return destination;
          }
        }
      }
    }
    
    if (routableModel == MeshModel.NO_ROUTING_MODEL) {  // TODO - maybe this should be an exception specific for this case instead of a general UnsupportedOperationException (so applications knows to catch it)
      throw new UnsupportedOperationException("Not a routed model, can not recommend route for distant node");
    }
    
    synchronized (knownMeshNodes) {
      RemoteNodeDetails rnd = knownMeshNodes.get(destination);
      if (rnd != null) {
        return rnd.stableNodeRoute;
      } else {
        return null;
      }
    }
  }

  public List<Address> disconnectAllNodes() {
    /*
     * By removing all known nodes, we can not communicate with them
     */
    List<Address> removedNodes = new ArrayList<Address>(connectedLeafs.size() + 
                                                        connectedPeers.size() +
                                                        knownMeshNodes.size());
    synchronized (connectedPeers) {
      synchronized (connectedLeafs) {
        synchronized (knownMeshNodes) {
          removedNodes.addAll(connectedPeers.keySet());
          connectedPeers.clear();
          removedNodes.addAll(connectedLeafs.keySet());
          connectedLeafs.clear();
          // we don't add KnownMeshNodes because they are not directly connected to us, and thus don't need a disconnect message
          knownMeshNodes.clear();
        }
      }
    }
    return Collections.unmodifiableList(removedNodes);
  }
  
  public void logRemoteNodeData() {
    if (log.isInfoEnabled()) {
      log.info("Connected nodes: " + getConnectedNodes());
      Iterator<Entry<Address, RemoteNodeDetails>> it2 = knownMeshNodes.entrySet().iterator();
      while (it2.hasNext()) {
        Entry<Address, RemoteNodeDetails> nodeEntry = it2.next();
        log.info("Details for node: " + nodeEntry.getKey());
        log.info("Routes via: " + nodeEntry.getValue().stableNodeRoute);
        log.info("Hops to us: " + nodeEntry.getValue().getHopsToUs());
        log.info("Peer connections: " + nodeEntry.getValue().connections.getPeerConnections());
        log.info("Leaf connections: " + nodeEntry.getValue().connections.getLeafConnections());
        log.info("Sample size: " + nodeEntry.getValue().count);
        log.info("Last update time: " + new Date(nodeEntry.getValue().lastUpdateTime));
      }
    }
  }

  public void logConnectionState() {
    if (log.isInfoEnabled()) {
      log.info("ConnectedPeers: " + connectedPeers.keySet());
      log.info("ConnectedLeafs: " + connectedLeafs.keySet());
      log.info("KnownMeshNodes: " + knownMeshNodes.keySet());
      log.info("ConnectableNodes: " + connectableNodes);
      log.info("ParentPeer: " + parentPeer);
    }
  }

  public boolean connectionClosed(Address nodeAddr) {
    boolean removedLeaf = connectedLeafs.remove(nodeAddr) != null; // we check leafs first, because that is the more likely disconnect case
    boolean removedPeer = false;
    if (! removedLeaf) {
      removedPeer = connectedPeers.remove(nodeAddr) != null;
      if (parentPeer != null && parentPeer.equals(nodeAddr)) {
        if (log.isInfoEnabled()) {
          log.info("Parent peer has cleanly disconnected");
        }
        parentPeer = null;
        
        if (safetyChecks && ! removedPeer) {
          if (log.isWarnEnabled()) {
            log.warn("Removed parent peer, but was not listed as a connected peer!!");
          }
          removedPeer = true; // should be true otherwise, but since it was not...we will set it
          new Exception().printStackTrace();
        }
      }
    }
    
    if (safetyChecks && removedLeaf && removedPeer) {
      if (log.isWarnEnabled()) {
        log.warn("Removed this peer, but the node was also listed as a leaf!!");
      }
    }
    return removedPeer || removedLeaf;
  }

  public boolean followPeerRecommendation(Address recommendedNode) {
    return ! connectedPeers.containsKey(recommendedNode);
  }

  public boolean canReachPeer(Address node) {
    return connectedPeers.containsKey(node) || 
           connectedLeafs.containsKey(node) || 
           knownMeshNodes.containsKey(node);
  }
}
