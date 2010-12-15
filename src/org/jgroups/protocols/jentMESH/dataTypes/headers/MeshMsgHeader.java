package org.jgroups.protocols.jentMESH.dataTypes.headers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

public class MeshMsgHeader extends MeshRoutedMsgHeader {
  private final Log log = LogFactory.getLog(getClass());
  private Address destination;  // null destination means broadcast to all nodes
  private int cachedSize;
  
  public MeshMsgHeader() {
    // should only be used before calling readFrom;
    cachedSize = -1;
  }
  
  private MeshMsgHeader(Address destination_addr) {
    cachedSize = -1;
    destination = destination_addr;
  }
  
  public MeshMsgHeader(Address destination_addr, Address local_addr, long nodeCurrTime, short avgHops, short peerConnections, short leafConnections) {
    this(destination_addr);
    addHop(local_addr, nodeCurrTime, avgHops, peerConnections, leafConnections);
  }
  
  public Address getDestination() {
    return destination;
  }
  
  @Override
  public int size() {
    if (cachedSize < 0) {
      cachedSize = (destination != null ? destination.size() : 1) + super.size(); // destination address + super (hopRecord)
    }
    return cachedSize;
  }

  @Override
  public void readFrom(DataInputStream in) throws IOException, 
                                                  IllegalAccessException,
                                                  InstantiationException {
    super.readFrom(in);
    destination = Util.readAddress(in);
  }

  @Override
  public void writeTo(DataOutputStream out) throws IOException {
    super.writeTo(out);
    Util.writeAddress(destination, out);
  }

  @Override
  public String toString() {
    return "MeshMsgHeader, destination: " + destination + ", hopRecord: " + hopRecord;
  }
}
