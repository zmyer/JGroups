package org.jgroups.protocols.jentMESH.dataTypes.headers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

public abstract class MeshRoutedMsgHeader extends TreeMeshHeader {
  private final Log log = LogFactory.getLog(getClass());
  private static Long nextID = new Long(0);
  private Long msgID = null;
  protected List<MsgHop> hopRecord;
  private List<Address> hopAddresses; // not transmitted, just used for CPU optimization
  private int cachedSize;
  private int cachedHopSize;  // used only to know if cachedSize is still accurate
  
  public MeshRoutedMsgHeader() {
    hopRecord = new ArrayList<MsgHop>(2);
    hopAddresses = new ArrayList<Address>(2);
    cachedSize = -1;
    cachedHopSize = -1;
    synchronized (nextID) {
      nextID++;
      msgID = new Long(nextID);
    }
  }
  
  public void addHop(Address local_addr, long nodeCurrTime, short avgHops, short peerConnections, short leafConnections) {
    if (local_addr == null) {
      throw new RuntimeException("local address can not be null!");
    }
    
    synchronized(hopRecord) {
      hopAddresses.add(local_addr);
      hopRecord.add(new MsgHop(local_addr, nodeCurrTime, avgHops, peerConnections, leafConnections));
    }
  }
  
  public Address getSender() {
    if (hopRecord.size() == 0) {
      throw new RuntimeException("This message has no sender");
    }
    return hopRecord.get(0).nodeAddress;
  }
  

  public abstract Address getDestination();
  
  public List<MsgHop> getHopRecord() {
    return Collections.unmodifiableList(hopRecord);
  }
  
  public List<Address> getHopsAddress() {
    // TODO - this safety check can be removed later
    if (hopAddresses.size() != hopRecord.size()) {  // used just to verify correctness, should they ever become out of sync somehow
      // update hopAddresses
      hopAddresses = new ArrayList<Address>(hopRecord.size() + 1);
      Iterator<MsgHop> it = hopRecord.iterator();
      while (it.hasNext()) {
        hopAddresses.add(it.next().nodeAddress);
      }
      if (log.isInfoEnabled()) {
        log.info("Had to update hopAddresses list....");
      }
    }
    return Collections.unmodifiableList(hopAddresses);
  }
  
  public Long getMsgID() {
    return msgID;
  }
  
  @Override
  public void readFrom(DataInputStream in) throws IOException, 
                                                  IllegalAccessException,
                                                  InstantiationException {
    msgID = new Long(in.readLong());
    short hopRecordSize = in.readShort();
    hopAddresses = new ArrayList<Address>(hopRecordSize + 1);
    hopRecord = new ArrayList<MsgHop>(hopRecordSize + 1);  // we add one because of the likelyhood that we may add a hop to this record
    for (int i = 0; i < hopRecordSize; i++) {
      MsgHop hopDetails = new MsgHop(in);
      hopRecord.add(hopDetails);
      hopAddresses.add(hopDetails.nodeAddress);
    }
  }

  @Override
  public void writeTo(DataOutputStream out) throws IOException {
    out.writeLong(msgID.longValue());
    synchronized (hopRecord) {
      out.writeShort(hopRecord.size());
      Iterator<MsgHop> it = hopRecord.iterator();
      while (it.hasNext()) {
        it.next().writeTo(out);
      }
    }
  }
  
  @Override
  public int size() {
    if (cachedHopSize == hopRecord.size()) {
      return cachedSize;
    }
    
    // msgID + hopRecord size
    int size = (Long.SIZE / 8) + (Short.SIZE / 8);
    synchronized (hopRecord) {
      // TODO - if we can be slightly less than accurate here, we should assume the address of every entry in the map is the same size
      Iterator<MsgHop> it = hopRecord.iterator();
      while (it.hasNext()) {
        size += it.next().size();
      }
      cachedHopSize = hopRecord.size();
      cachedSize = size;
    }
    return size;
  }
  
  public class MsgHop implements Streamable {
    public Address nodeAddress;
    public long nodeTime; // node time may not be needed, but it is left in currently to ensure that the MeshModel does not try to read old information (in case messages are out of order)
    public short avgHops;
    public short peerConnections;
    public short leafConnections;
    private static final int staticSize = (Long.SIZE / 8) + (3 * (Short.SIZE / 8));  // node time + 3 shorts
    
    public MsgHop(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
      readFrom(in);
    }
    
    public MsgHop(Address nodeAddress, long nodeCurrTime, short avgHops, short peerConnections, short leafConnections) {
      if (nodeAddress == null) {
        throw new RuntimeException("Hop address can not be null!");
      }
      
      this.nodeAddress = nodeAddress;
      nodeTime = nodeCurrTime;
      this.avgHops = avgHops;
      this.peerConnections = peerConnections;
      this.leafConnections = leafConnections;
    }
    
    public int size() {
      return staticSize + nodeAddress.size();
    }

    @Override
    public void readFrom(DataInputStream in) throws IOException, 
                                                    IllegalAccessException,
                                                    InstantiationException {
      nodeAddress = Util.readAddress(in);
      nodeTime = in.readLong();
      avgHops = in.readShort();
      peerConnections = in.readShort();
      leafConnections = in.readShort();
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
      Util.writeAddress(nodeAddress, out);
      out.writeLong(nodeTime);
      out.writeShort(avgHops);
      out.writeShort(peerConnections);
      out.writeShort(leafConnections);
    }

    @Override
    public String toString() {
      return nodeAddress.toString();
    }
  }
}
