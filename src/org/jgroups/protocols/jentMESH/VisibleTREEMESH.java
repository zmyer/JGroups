package org.jgroups.protocols.jentMESH;

import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.annotations.Property;

public class VisibleTREEMESH extends TREEMESH {
  /* ------------------------------------------    Properties     ---------------------------------------------- */
  @Property(description="Time delay between checks to establish new connections", writable=false)
  protected int PORT = 28241;
  protected String BIND_ADDRESS = "0.0.0.0";

  /* ------------------------------------------     Commands      ---------------------------------------------- */
  public static final String isStable = "isStable";
  public static final String getConStats = "getConStats";
  public static final String modelConnectionDesire = "modelConnectionDesire";
  public static final String modelRemoteNodeStats = "modelRemoteNodeStats";
  public static final String listConnectedNodes = "listConnectedNodes";
  public static final String listAllNodes = "listAllNodes";
  public static final String addNodeBlacklist = "addNodeBlacklist";
  public static final String addNodeBlacklistIdx = "addNodeBlacklistIdx";
  public static final String addNodeBlacklistStr = "addNodeBlacklistStr";
  public static final String clearNodeBlacklist = "clearNodeBlacklist";
  public static final String disconnectNode = "disconnectNode";
  public static final String disconnectNodeIdx = "disconnectNodeIdx";
  public static final String disconnectNodeStr = "disconnectNodeStr";
  public static final String help = "help";
  public static final String setVerboseLevel0 = "setVerboseLevel0";
  public static final String setVerboseLevel1 = "setVerboseLevel1";
  public static final String setVerboseLevel2 = "setVerboseLevel2";
  public static final String setVerboseLevel3 = "setVerboseLevel3";
  
  /* ---------------------------------------------    Fields    ------------------------------------------------ */
  private Server visbilityServer;
  @Override
  public void init() {
    super.init();
    visbilityServer = new Server();
    visbilityServer.start();
  }

  @Override
  public void destroy() {
    visbilityServer.stop();
    super.destroy();
  }
  
  private class Server implements Runnable {
    private Thread thread;
    private List<Address> lastReturnedAllNodes = null;
    private List<Address> lastReturnedConnectedPeers = null;
    private List<Address> lastReturnedConnectedLeafs = null;

    public synchronized void start() {
      if (thread == null) {
        thread = new Thread(this, "VisibleTREEMESH.Listener");
        thread.setDaemon(true);
        thread.start();
      }
    }

    public synchronized void stop() {
      thread = null;
    }

    public void run() {
      ServerSocket server = null;
      try {
        server = new ServerSocket();
        server.bind(new InetSocketAddress(BIND_ADDRESS, PORT));
        server.setSoTimeout(1000);
        log.info("listening on " + BIND_ADDRESS + ":" + PORT);
        while (thread != null) {
          try {
            Socket socket = server.accept();
              
            try {
              InputStream in = socket.getInputStream();
              String command = "";
              String arguments = "";
              int val;
              while ((val = in.read()) != -1 && val != '\n' && val != '\0') {
                command += (char)val;
              }
              
              int spaceIndex = command.indexOf(' ');
              if (spaceIndex > 0) {
                arguments = command.substring(spaceIndex + 1).trim();
                command = command.substring(0, spaceIndex).toLowerCase().trim();
              } else {
                command = command.toLowerCase().trim();
              }
              
              if (command.equalsIgnoreCase(isStable)) {
                log.info("Model Stablility: " + meshModel.isModelStable() + 
                         ", remote nodes: " + meshModel.getRemoteNodes().size());
              } else if (command.equalsIgnoreCase(getConStats)) {
                meshModel.logConnectionState();
              } else if (command.equalsIgnoreCase(modelConnectionDesire)) {
                log.info("Make peer connection: " + meshModel.makePeerConnection());
                log.info("Make leaf connection: " + meshModel.makeLeafConnection(peerConMax));
                log.info("Need to merge: " + meshModel.needToMerge(peerConMax));
              } else if (command.equalsIgnoreCase(modelRemoteNodeStats)) {
                meshModel.logRemoteNodeData();
              } else if (command.equalsIgnoreCase(listAllNodes)) {
                List<Address> connectedNodes = meshModel.getConnectedNodes();
                List<Address> remoteNodes = meshModel.getRemoteNodes();
                lastReturnedAllNodes = new ArrayList<Address>(connectedNodes.size() + 
                                                              remoteNodes.size());
                lastReturnedAllNodes.addAll(connectedNodes);
                lastReturnedAllNodes.addAll(remoteNodes);
                for (int i = 0; i < lastReturnedAllNodes.size(); i++) {
                  log.info("[" + (i + 1) + "] - " + lastReturnedAllNodes.get(i));
                }
              } else if (command.equalsIgnoreCase(addNodeBlacklist)) {
                if (validBlacklistArgument(arguments)) {
                  try {
                    // try the parse, if it works, treat as index, if throws exception then treat as a string
                    Integer.parseInt(arguments);
                    blacklistNodeBasedOnIndex(arguments);
                  } catch (NumberFormatException e) {
                    blacklistNodeBasedOnString(arguments);
                  }
                }
              } else if (command.equalsIgnoreCase(addNodeBlacklistIdx)) {
                if (validBlacklistArgument(arguments)) {
                  blacklistNodeBasedOnIndex(arguments);
                }
              } else if (command.equalsIgnoreCase(addNodeBlacklistStr)) {
                if (validBlacklistArgument(arguments)) {
                  blacklistNodeBasedOnString(arguments);
                }
              } else if (command.equalsIgnoreCase(clearNodeBlacklist)) {
                List<Address> removedBlacklistNodes = conManager.clearBlacklist();
                log.info("Blacklist cleared..." +
                         "the following nodes were removed from the blacklist: " + 
                         removedBlacklistNodes);
              } else if (command.equalsIgnoreCase(listConnectedNodes)) {
                lastReturnedConnectedPeers = meshModel.getConnectedPeers();
                lastReturnedConnectedLeafs = meshModel.getConnectedLeafs();
                int i = 1;
                Iterator<Address> it = lastReturnedConnectedPeers.iterator();
                while (it.hasNext()) {
                  log.info("[" + i++ + "] - " + it.next() + " (Peer)");
                }
                it = lastReturnedConnectedLeafs.iterator();
                while (it.hasNext()) {
                  log.info("[" + i++ + "] - " + it.next() + " (Leaf)");
                }
              } else if (command.startsWith(disconnectNodeIdx.toLowerCase())) {
                disconnectNodeBasedOnIndex(arguments);
              } else if (command.startsWith(disconnectNodeStr.toLowerCase())) {
                disconnectNodeBasedOnString(arguments);
              } else if (command.startsWith(disconnectNode.toLowerCase())) {
                if (arguments.equals("")) {
                  log.warn("You must specify the node string or index the node to disconnect given by the command: " + 
                           listConnectedNodes);
                  log.warn("Syntax: " + disconnectNode + " [node string|index]");
                } else if (arguments.indexOf(' ') >= 0) {
                  log.warn(disconnectNode + " command only accepts one argument at a time");
                } else {
                  try {
                    // try the parse, if it works, treat as index, if throws exception then treat as a string
                    Integer.parseInt(arguments);
                    disconnectNodeBasedOnIndex(arguments);
                  } catch (NumberFormatException e) {
                    disconnectNodeBasedOnString(arguments);
                  }
                }
              } else if (command.equalsIgnoreCase(setVerboseLevel0)) {
                verbose = false;
                msgVerbose = false;
                highVerbose = false;
                log.info("Verbosity turned off");
              } else if (command.equalsIgnoreCase(setVerboseLevel1)) {
                verbose = true;
                msgVerbose = false;
                highVerbose = false;
                log.info("Verbosity set to level 1 (light verbose)");
              } else if (command.equalsIgnoreCase(setVerboseLevel2)) {
                verbose = true;
                msgVerbose = true;
                highVerbose = false;
                log.info("Verbosity set to level 2 (light verbose + msg verbose)");
              } else if (command.equalsIgnoreCase(setVerboseLevel3)) {
                verbose = true;
                msgVerbose = true;
                highVerbose = true;
                log.info("Verbosity set to level 3 (high verbose + msg verbose)");
              } else if (command.equalsIgnoreCase(help)) {
                log.info("command: " + isStable);
                log.info("\tDisplays if the node considers its model stable, " +
                		     "and how many remote nodes it is tracking");
                log.info("command: " + getConStats);
                log.info("\tDisplays the models current connection state");
                log.info("\t\t(connected peers, leafs, remote nodes, parent peer)");
                log.info("command: " + modelConnectionDesire);
                log.info("\tDisplays the models requests for new connections");
                log.info("command: " + modelRemoteNodeStats);
                log.info("\tDisplays collected information about remote nodes");
                log.info("command: " + listAllNodes);
                log.info("\tLists all known nodes (connected or not)");
                log.info("command: " + addNodeBlacklist + "[string|index]");
                log.info("\tAdd a node to be blacklisted (and should never connect to them)");
                log.info("\t\tNote that " + listAllNodes + 
                         " must be run before blacklisting a node");
                log.info("command: " + addNodeBlacklistIdx + "[index]");
                log.info("\tAdd a node to be blacklisted (and should never connect to them)");
                log.info("\t\tNote that " + listAllNodes + 
                         " must be run before blacklisting a node");
                log.info("command: " + addNodeBlacklistStr + "[string]");
                log.info("\tAdd a node to be blacklisted (and should never connect to them)");
                log.info("\t\tNote that " + listAllNodes + 
                         " must be run before blacklisting a node");
                log.info("command: " + clearNodeBlacklist);
                log.info("\tRemoves any nodes currently in the blacklist");
                log.info("command: " + listConnectedNodes);
                log.info("\tLists all nodes connected to us (and if peer or leaf)");
                log.info("command: " + disconnectNode + "  [string|index]");
                log.info("\tDisconnect a node based on their string or index");
                log.info("\t\tNote that " + listConnectedNodes + 
                         " must be run before disconnecting based on index");
                log.info("command: " + disconnectNodeIdx + " [index]");
                log.info("\tDisconnects a node based on the index listed from command " + 
                         listConnectedNodes);
                log.info("command: " + disconnectNodeStr  + " [string]");
                log.info("\tDisconnects a node based on the string of their name");
                log.info("command: " + setVerboseLevel0);
                log.info("\tSets all verbosit off");
                log.info("command: " + setVerboseLevel1);
                log.info("\tTurns on light verbosity");
                log.info("command: " + setVerboseLevel2);
                log.info("\tTurns on light verbosity, with also message verbosity");
                log.info("command: " + setVerboseLevel3);
                log.info("\tTurns on heavy verbosity, also with message verbosity");
              } else {
                log.warn("Unknown command: " + command + (arguments.trim().equals("") ? "" : ", args: " + arguments));
              }
            } finally {
              socket.close();
            }
          } catch (SocketTimeoutException ok) { }
        }
      } catch (BindException e) {
        if (log.isWarnEnabled()) {
          log.warn("unable to bind to " + BIND_ADDRESS + ":" + PORT);
        }
      } catch (IOException e) {
        if (log.isWarnEnabled()) {
          log.warn(null, e);
        }
      } finally {
        try {
          if (server != null) server.close();
        } catch (IOException ignored) { }
      }
    }
    
    private boolean validBlacklistArgument(String arguments) {
      if (arguments.equals("")) {
        log.warn("You must specify the node string or index the node to disconnect given by the command: " + 
                 listConnectedNodes);
        log.warn("Syntax: " + disconnectNode + " [node string|index]");
        return false;
      } else if (arguments.indexOf(' ') >= 0) {
        log.warn(disconnectNode + " command only accepts one argument at a time");
        return false;
      } else if (lastReturnedAllNodes == null) {
        log.warn("You must run the command: " + listAllNodes + 
                 " before you can blacklist any nodes");
        return false;
      } else {
        return true;
      }
    }
    
    private void blacklistNodeBasedOnIndex(String index) {
      try {
        int nodeIndex = Integer.parseInt(index);
        if (nodeIndex <= lastReturnedAllNodes.size()) {
          nodeIndex -= 1;
          Address disconnectNode = lastReturnedAllNodes.get(nodeIndex);
          if (conManager.addBlacklist(disconnectNode)) {
            log.info("Added blacklist for node: " + disconnectNode);
          } else {
            log.info("Node " + disconnectNode + " is already blacklisted");
          }
        } else {
          log.warn("Input node index is larger than our connected node list: " + 
                   lastReturnedAllNodes.size());
        }
      } catch (NumberFormatException e) {
        log.warn("Could not parse index of node to blacklist, index passed in: " + index);
      } catch (Throwable t) {
        log.warn("", t);
      }
    }
    
    private void blacklistNodeBasedOnString(String nodeStr) {
      boolean foundNode = false;
      Iterator<Address> it = lastReturnedAllNodes.iterator();
      while (it.hasNext()) {
        Address node = it.next();
        if (node.toString().equals(nodeStr)) {
          foundNode = true;
          if (conManager.addBlacklist(node)) {
            log.info("Adding blacklist for node: " + nodeStr);
          } else {
            log.info("Node " + nodeStr + " is already blacklisted");
          }
          break;
        }
      }
      if (! foundNode) {
        log.warn("Could not find a node to blacklist with the string: " + nodeStr);
      }
    }
    
    private void disconnectNodeBasedOnIndex(String index) {
      if (lastReturnedConnectedPeers == null || lastReturnedConnectedLeafs == null) {
        log.warn("Can not disconnect a node without listing node indexes, please run the command: " + 
                 listConnectedNodes);
      } else if (index.equals("")) {
        log.warn("You must specify the index of the node to disconnect given by the command: " + 
                 listConnectedNodes);
        log.warn("Syntax: " + disconnectNodeIdx + " [index]");
      } else if (index.indexOf(' ') >= 0) {
        log.warn(disconnectNodeIdx + " command only accepts one argument at a time");
      } else {
        try {
          int nodeIndex = Integer.parseInt(index);
          if (nodeIndex <= lastReturnedConnectedPeers.size()) {
            nodeIndex -= 1;
            Address disconnectNode = lastReturnedConnectedPeers.get(nodeIndex);
            if (meshModel.connectionClosed(disconnectNode)) {
              conManager.sendNodeDisconnectMsg(disconnectNode);
              log.info("Disconnected peer node: " + disconnectNode);
            } else {
              log.info("Not connected to peer node: " + disconnectNode + 
                       " (try updating the connection list using command: " + listConnectedNodes + ")");
            }
          } else if (nodeIndex - lastReturnedConnectedPeers.size() <= lastReturnedConnectedLeafs.size()) {
            nodeIndex -= (lastReturnedConnectedPeers.size() + 1);
            Address disconnectNode = lastReturnedConnectedLeafs.get(nodeIndex);
            if (meshModel.connectionClosed(disconnectNode)) {
              conManager.sendNodeDisconnectMsg(disconnectNode);
              log.info("Disconnecting leaf node: " + disconnectNode);
            } else {
              log.info("Not connected to leaf node: " + disconnectNode + 
                       " (try updating the connection list using command: " + listConnectedNodes + ")");
            }
          } else {
            log.warn("Input node index is larger than our connected node list: " + 
                     (lastReturnedConnectedPeers.size() + lastReturnedConnectedLeafs.size()));
          }
        } catch (NumberFormatException e) {
          log.warn("Could not parse index of node to disconnect, index passed in: " + index);
        } catch (Throwable t) {
          log.warn("", t);
        }
      }
    }
    
    private void disconnectNodeBasedOnString(String nodeStr) {
      if (nodeStr.equals("")) {
        log.warn("You must specify the node string of the node to disconnect given by the command: " + 
                 listConnectedNodes);
        log.warn("Syntax: " + disconnectNodeStr + " [node string]");
      } else if (nodeStr.indexOf(' ') >= 0) {
        log.warn(disconnectNodeStr + " command only accepts one argument at a time");
      } else {
        boolean foundNode = false;
        Iterator<Address> it = meshModel.getConnectedNodes().iterator();
        while (it.hasNext()) {
          Address node = it.next();
          if (node.toString().equals(nodeStr)) {
            foundNode = true;
            log.info("Disconnecting node: " + nodeStr);
            if (meshModel.connectionClosed(node)) {
              conManager.sendNodeDisconnectMsg(node);
            }
            break;
          }
        }
        if (! foundNode) {
          log.warn("Could not find a connected node to disconnect with the string: " + nodeStr);
          log.warn("Currently connected nodes: " + meshModel.getConnectedNodes());
        }
      }
    }
  }
}
