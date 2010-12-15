package org.jgroups.protocols.jentMESH.dataTypes.headers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.jgroups.Address;
import org.jgroups.util.Util;

public class MeshHeartbeatHeader extends MeshRoutedMsgHeader {
  public MeshHeartbeatHeader() {
    super();
  }
  
  public MeshHeartbeatHeader(Address local_addr, long nodeCurrTime, short avgHops, short peerConnections,
                         short leafConnections) {
    super.addHop(local_addr, nodeCurrTime, avgHops, peerConnections, leafConnections);
  }

  @Override
  public int size() {
    return super.size();
  }

  @Override
  public void readFrom(DataInputStream in) throws IOException, 
                                                  IllegalAccessException,
                                                  InstantiationException {
    super.readFrom(in);
  }

  @Override
  public void writeTo(DataOutputStream out) throws IOException {
    super.writeTo(out);
  }

  @Override
  public Address getDestination() {
    return null;
  }

  @Override
  public String toString() {
    return "MeshHeartbeatHeader, hopRecord: " + hopRecord;
  }
}
