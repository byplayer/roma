package jp.co.rakuten.rit.roma.command.java;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import jp.co.rakuten.rit.roma.command.Command;
import jp.co.rakuten.rit.roma.command.Receiver;
import jp.co.rakuten.rit.roma.routing.RoutingTable;
import jp.co.rakuten.rit.roma.storage.BasicStorage;
import jp.co.rakuten.rit.roma.storage.DataEntry;

public class StorageCommands {
    private static final String CRLF = "\r\n";

    public static long getHash(String key) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA1");
            ByteBuffer digest = ByteBuffer.wrap(md.digest(key.getBytes()));
            digest.position(12);
            long d = digest.getLong() << 32 >>> 32;
            return d;
        } catch (NoSuchAlgorithmException e) {
        }
        return 0;
    }

    public static class GetCommand extends Command {
        @Override
        public String getName() {
            return "javaget";
        }

        @Override
        public Object execute(Receiver receiver, String[] commands)
                throws Exception {
            String[] keys = commands[1].split("\\e");
            String key = keys[0];
            String hashName;
            if (keys.length == 1) {
                hashName = "roma";
            } else {
                hashName = keys[1];
            }

            RoutingTable rttable = receiver.getRoutingTable();
            long hash = getHash(key);
            long vnodeID = rttable.getVirtualNodeID(hash);
            List<String> nodeIDs = rttable.searchNodeIDs(vnodeID);

            // TODO
            // if (!nodeIDs.contains(receiver.getLocalNodeID())) {
            // System.out.println("forwarding error");
            // return null;
            // }

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
}