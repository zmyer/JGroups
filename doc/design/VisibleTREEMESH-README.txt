The VisibleTREEMESH protocol is just an extension of functionality from the TREEMESH protocol.  It allows for commands to be sent to the node and have it output additional information about its state to the log.

It can be run by using the following command:
echo "[command] <args>" | nc localhost 28241

echo the string and pipe it into netcat to the node on the listening port
port 28241 is the default port specified in VisibleTREEMESH.java

you can see a full list of commands by running:
echo "help" | nc localhost 28241
