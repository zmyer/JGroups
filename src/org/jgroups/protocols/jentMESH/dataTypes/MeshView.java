package org.jgroups.protocols.jentMESH.dataTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.Util;

public class MeshView extends View {
  protected List<Address> directlyConnectedNodes;
  protected List<Address> remoteMeshNodes;
  private int cachedSize;
  
  public MeshView(Address creator, long id, List<Address> connectedNodes, List<Address> remoteMeshNodes) {
    this(new ViewId(creator, id), connectedNodes, remoteMeshNodes);
  }
  
  public MeshView(ViewId vid, List<Address> connectedNodes, List<Address> remoteMeshNodes) {
    cachedSize = -1;
    this.vid = vid;
    this.directlyConnectedNodes = Collections.unmodifiableList(connectedNodes);
    this.remoteMeshNodes = Collections.unmodifiableList(remoteMeshNodes);
  }
  
  public MeshView() { // should only be called before readFrom
    cachedSize = -1;
  }

  @Override
  public Vector<Address> getMembers() {
    Vector<Address> members = new Vector<Address>(directlyConnectedNodes.size() + 
                                                  remoteMeshNodes.size() + 1);
    members.add(this.vid.getCoordAddress());
    members.addAll(directlyConnectedNodes);
    members.addAll(remoteMeshNodes);
    return members;
  }
  
  public List<Address> getConnectedMembers() {
    return directlyConnectedNodes;
  }
  
  public List<Address> getRemoteMembers() {
    return remoteMeshNodes;
  }

  @Override
  public boolean containsMember(Address member) {
    return connectedToMember(member) || 
           remoteMeshNodes.contains(member);
  }
  
  public boolean connectedToMember(Address member) {
    return directlyConnectedNodes.contains(member);
  }

  @Override
  public boolean equals(Object input) {
    if (this == input) {
      return true;
    } else if (input instanceof MeshView) {
      return equals((MeshView)input);
    } else {
      return false;
    }
  }
  
  public boolean equals(MeshView input) {
    if ((vid != null && vid.compareTo(input.vid) != 0) ||
        (vid == null && input.vid != null)) {
      return false;
    }
    if ((directlyConnectedNodes != null && 
         ! directlyConnectedNodes.equals(input.directlyConnectedNodes)) ||
        (directlyConnectedNodes == null && input.directlyConnectedNodes != null)) {
      return false;
    }
    if ((remoteMeshNodes != null && ! remoteMeshNodes.equals(input.remoteMeshNodes)) ||
        (remoteMeshNodes == null && input.remoteMeshNodes != null)) {
      return false;
    }
    
    return true;
  }

  @Override
  public int size() {
    return directlyConnectedNodes.size() + remoteMeshNodes.size();
  }

  @Override
  public String toString() {
    return vid + " " + directlyConnectedNodes + " - " + remoteMeshNodes;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeObject(directlyConnectedNodes);
    out.writeObject(remoteMeshNodes);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    directlyConnectedNodes = (List<Address>)in.readObject();
    remoteMeshNodes = (List<Address>)in.readObject();
  }
  
  @Override
  public void writeTo(DataOutputStream out) throws IOException {
    super.writeTo(out);
    
    out.writeInt(directlyConnectedNodes.size());
    Iterator<Address> it = directlyConnectedNodes.iterator();
    while (it.hasNext()) {
      Util.writeAddress(it.next(), out);
    }
    
    out.writeInt(remoteMeshNodes.size());
    it = remoteMeshNodes.iterator();
    while (it.hasNext()) {
      Util.writeAddress(it.next(), out);
    }
  }

  @Override
  public void readFrom(DataInputStream in) throws IOException, 
                                                  IllegalAccessException, 
                                                  InstantiationException {
    super.readFrom(in);
    
    int connectedNodeCount = in.readInt();
    directlyConnectedNodes = new ArrayList<Address>(connectedNodeCount);
    for (int i = 0; i < connectedNodeCount; i++) {
      directlyConnectedNodes.add(Util.readAddress(in));
    }
    
    int remoteNodeCount = in.readInt();
    remoteMeshNodes = new ArrayList<Address>(remoteNodeCount);
    for (int i = 0; i < remoteNodeCount; i++) {
      remoteMeshNodes.add(Util.readAddress(in));
    }
  }

  @Override
  public int serializedSize() {
    if (cachedSize >= 0) {
      return cachedSize;
    }
    
    int size = super.serializedSize();
    
    size += Integer.SIZE / 8; // qty of directly connected nodes
    Iterator<Address> it = directlyConnectedNodes.iterator();
    while (it.hasNext()) {
      size += it.next().size();
    }
    
    size += Integer.SIZE / 8; // qty of remoteMeshNodes
    it = remoteMeshNodes.iterator();
    while (it.hasNext()) {
      size += it.next().size();
    }
    
    cachedSize = size;
    return size;
    //return Util.sizeOf(this);
  }
}
