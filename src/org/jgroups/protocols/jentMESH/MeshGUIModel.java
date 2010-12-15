package org.jgroups.protocols.jentMESH;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.jentMESH.dataTypes.MeshIdentifier;
import org.jgroups.protocols.jentMESH.dataTypes.NodeConnectionDetails;
import org.jgroups.protocols.jentMESH.dataTypes.headers.MeshRoutedMsgHeader.MsgHop;
import org.jgroups.protocols.jentMESH.util.Clock;
import org.jgroups.util.TimeScheduler;

public class MeshGUIModel extends MeshModel {
  private static final int writeWait = 1000;  // delay in ms between writes
  
  private final Log log = LogFactory.getLog(getClass());
  private long lastWriteTime;
  private File outputFile;
  private MeshIdentifier self;
  
  public MeshGUIModel(short sampleSize, short routableMeshModel, long rootStickyTime, String filename, MeshIdentifier self, TimeScheduler timer) {
    super(sampleSize, routableMeshModel, rootStickyTime);
    
    this.self = self;
    
    outputFile = new File(filename);
    if (outputFile.exists()) {
      System.out.println("File already exists, wont overwrite: " + filename);
      System.exit(1);
    }
    outputFile.deleteOnExit();
    
    if (log.isInfoEnabled()) {
      log.info("Writing status to: " + outputFile.getAbsolutePath());
    }
    
    // schedule a task to make sure that updates are current
    timer.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (Clock.updateAndGetCurrTime() - lastWriteTime >= writeWait) {
          write();
        }
      }
    }, writeWait, writeWait, TimeUnit.MILLISECONDS);
  }
  
  private void write() {
    BufferedWriter out = null;
    lastWriteTime = Clock.nowInMilliseconds();  // this does not need to be perfect, a rough estimate is fine
    try {
      out = new BufferedWriter(new FileWriter(outputFile));
      
      out.write("id:" + self.toString() + ";\n");
      
      Iterator<Address> it = connectedPeers.keySet().iterator();
      while (it.hasNext()) {
        Address peer = it.next();
        if (parentPeer != null && peer.equals(parentPeer)) {
          out.append("parentPeer:" + peer + ";\n");
        } else {
          out.append("peer:" + peer + ";\n");
        }
      }
      
      it = connectedLeafs.keySet().iterator();
      while (it.hasNext()) {
        out.append("leaf:" + it.next() + ";\n");
      }
      
      out.flush();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  @Override
  public boolean connectionClosed(Address src) {
    boolean superResult = super.connectionClosed(src);
    
    if (superResult) {
      write();
    }
    
    return superResult;
  }
  
  @Override
  public List<Address> disconnectAllLeafs() {
    List<Address> superResult = super.disconnectAllLeafs();
    
    if (! superResult.isEmpty()) {
      write();
    }
    
    return superResult;
  }
  
  @Override
  public List<Address> disconnectAllNodes() {
    List<Address> superResult = super.disconnectAllNodes();
    
    if (! superResult.isEmpty()) {
      write();
    }
    
    return superResult;
  }
  
  @Override
  public boolean timeoutDeadNodes(int timeout) {
    boolean superResult = super.timeoutDeadNodes(timeout);
    
    if (superResult) {
      write();
    }
    
    return superResult;
  }

  @Override
  public void newLeafConnection(Address newLeaf, 
                                short peerConnections, short leafConnections) {
    super.newLeafConnection(newLeaf, peerConnections, leafConnections);
    write();
  }

  @Override
  public void newPeerConnection(Address newPeer, short peerConnections, short leafConnections, 
                                boolean possibleParent) {
    super.newPeerConnection(newPeer, peerConnections, leafConnections, possibleParent);
    write();
  }
}
