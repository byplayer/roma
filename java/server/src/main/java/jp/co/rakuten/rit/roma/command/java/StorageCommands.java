package jp.co.rakuten.rit.roma.command.java;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.co.rakuten.rit.roma.command.Command;
import jp.co.rakuten.rit.roma.command.ErrorMessage;
import jp.co.rakuten.rit.roma.command.Receiver;
import jp.co.rakuten.rit.roma.routing.RoutingTable;
import jp.co.rakuten.rit.roma.storage.BasicStorage;
import jp.co.rakuten.rit.roma.storage.DataEntry;

public class StorageCommands {
    private static Logger LOG = LoggerFactory.getLogger(StorageCommands.class);

    private static final String CRLF = "\r\n";

    public static long getHash(String key) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA1");
            ByteBuffer digest = ByteBuffer.wrap(md.digest(key.getBytes()));
            digest.position(12);
            return digest.getLong() << 32 >>> 32;
        } catch (NoSuchAlgorithmException e) {
        }
        return 0;
    }

    public static byte[] forwardGet(Receiver receiver, String[] commands)
            throws Exception {
        LOG.warn("forward get " + commands[1]);
        // TODO
        return null;
    }

    public static Object forwardGets(Receiver receiver, String[] commands)
            throws Exception {
        return null;
    }

    public static class GetCommand extends Command implements ErrorMessage {
        @Override
        public String getAliasName() {
            return "javaget";
        }

        @Override
        public String getName() {
            return "ev_javaget";
        }

        @Override
        public Object execute(Receiver receiver, String[] commands)
                throws Exception {
            if (commands.length > 2) {
                return GetsCommand.execute0(receiver, commands);
            }

            String[] keys = commands[1].split("\\e");
            String key = keys[0];
            String hashName;
            if (keys.length == 1) {
                hashName = receiver.getDefaultHashName();
            } else {
                hashName = keys[1];
            }

            RoutingTable rttable = receiver.getRoutingTable();
            long hash = getHash(key);
            long vnodeID = rttable.getVirtualNodeID(hash);
            List<String> nodeIDs = rttable.searchNodeIDs(vnodeID);

            if (!nodeIDs.contains(receiver.getLocalNodeID())) {
                byte[] res = forwardGet(receiver, commands);
                if (res != null) {
                    receiver.writeBytes(res);
                } else {
                    receiver.writeString(SERVER_ERROR_FORWARD);
                }
                return null;
            }

            Map<String, BasicStorage> map = receiver.getStorages();
            BasicStorage storage = map.get(hashName);
            if (storage == null) {
                System.out.println("cannot storage error");
                return null;
            }

            DataEntry e1 = storage.createDataEntry(key, vnodeID, 0, 0, 0, null);
            DataEntry e2 = storage.execGetCommand(e1);
            // TODO read count up
            if (e2 != null) {
                byte[] value = e2.getValue();
                StringBuilder sb = new StringBuilder();
                sb.append("VALUE ");
                sb.append(key);
                sb.append(" 0 ");
                sb.append(value.length);
                sb.append(CRLF);
                receiver.writeString(sb.toString());
                receiver.writeBytes(value);
                receiver.writeString(CRLF);
            }
            receiver.writeString("END" + CRLF);
            return null;
        }
    }

    public static class GetsCommand extends Command {
        @Override
        public String getAliasName() {
            return "ev_javagets";
        }

        @Override
        public String getName() {
            return "javagets";
        }

        @Override
        public Object execute(Receiver receiver, String[] commands)
                throws Exception {
            return execute0(receiver, commands);
        }

        private static void addList(HashMap<String, List<String>> nodeID2Keys,
                String ID, String key) {
            List<String> keys = nodeID2Keys.get(ID);
            if (keys != null) {
                keys = new ArrayList<String>();
                nodeID2Keys.put(ID, keys);
            }
            keys.add(key);
        }

        public static Object execute0(Receiver receiver, String[] commands)
                throws Exception {
            // node, list<key>
            HashMap<String, List<String>> nodeID2Keys = new HashMap<String, List<String>>();
            HashMap<String, DataEntry> ret = new HashMap<String, DataEntry>();
            List<String> retKeyList = new ArrayList<String>();
            Map<String, BasicStorage> map = receiver.getStorages();
            RoutingTable rttable = receiver.getRoutingTable();
            for (int i = 1; i < commands.length; ++i) {
                String[] keys = commands[i].split("\\e");
                String key = keys[0];
                String hName;
                if (keys.length == 1) {
                    hName = receiver.getDefaultHashName();
                } else {
                    hName = keys[1];
                }
                long hash = getHash(key);
                long vnodeID = rttable.getVirtualNodeID(hash);
                List<String> nodeIDs = rttable.searchNodeIDs(vnodeID);
                String localNodeID = receiver.getLocalNodeID();
                if (!nodeIDs.contains(localNodeID)) {
                    addList(nodeID2Keys, nodeIDs.get(0), commands[i]);
                } else { // contains localNodeID
                    BasicStorage storage = map.get(hName);
                    if (storage == null) {
                        System.out.println("cannot storage error");
                        continue;
                    }
                    // TODO read count up
                    DataEntry e1 = storage.createDataEntry(key, vnodeID, 0, 0,
                            0, null);
                    DataEntry e2 = storage.execGetCommand(e1);
                    ret.put(commands[i], e2);
                }
                retKeyList.add(key);
            }

            // other nodes
            // TODO

            Iterator<String> retKeys = retKeyList.iterator();
            while (retKeys.hasNext()) {
                DataEntry e = ret.get(retKeys.next());
                if (e == null) {
                    continue;
                }

                byte[] value = e.getValue();
                StringBuilder sb = new StringBuilder();
                sb.append("VALUE ");
                sb.append(e.getKey());
                sb.append(" 0 ");
                sb.append(value.length);
                sb.append(CRLF);
                receiver.writeString(sb.toString());
                receiver.writeBytes(value);
                receiver.writeString(CRLF);
            }
            receiver.writeString("END" + CRLF);
            return null;
        }
    }
}