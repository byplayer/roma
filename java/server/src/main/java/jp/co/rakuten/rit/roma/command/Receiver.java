package jp.co.rakuten.rit.roma.command;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jp.co.rakuten.rit.roma.event.HandlerBase;
import jp.co.rakuten.rit.roma.routing.RoutingTable;
import jp.co.rakuten.rit.roma.storage.BasicStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Receiver {
    private static final Logger LOG = LoggerFactory.getLogger(Receiver.class);

    private static ConcurrentMap<String, String> keys = new ConcurrentHashMap<String, String>();

    private HandlerBase handler;

    private Session sess;

    private Map<String, BasicStorage> storages;

    private RoutingTable routingTable;

    public Receiver(HandlerBase handler, Session sess) {
        this.handler = handler;
        this.sess = sess;
    }

    public Session getSession() {
        return sess;
    }

    public void setCommands(String commandLine) {
        sess.setCommands(commandLine);
    }

    public Connection getConnection(String nodeID) throws IOException {
        return handler.getConnectionPool().get(nodeID);
    }

    public void putConnection(String nodeID, Connection conn)
            throws IOException {
        handler.getConnectionPool().put(nodeID, conn);
    }

    public String getCommandName(String aliasName) {
        return handler.getCommandMap(aliasName);
    }

    public void stopEventLoop() {
        handler.stopService();
    }

    public void setStorages(Map<String, BasicStorage> storages) {
        this.storages = storages;
    }

    public Map<String, BasicStorage> getStorages() {
        return storages;
    }

    public void setRoutingTable(RoutingTable routingTable) {
        this.routingTable = routingTable;
    }

    public RoutingTable getRoutingTable() {
        return routingTable;
    }

    public byte[] readBytes(int len) throws IOException {
        return sess.readBytes(len);
    }

    public String gets() throws IOException {
        return sess.gets();
    }

    public String readLine() throws IOException {
        return sess.readLine();
    }

    public String blockingReadLine() throws IOException {
        return sess.blockingReadLine();
    }

    public void writeBytes(byte[] bytes) throws IOException {
        sess.writeBytes(bytes);
    }

    public void writeString(String data) throws IOException {
        sess.writeString(data);
    }

    public int execCommand() throws Exception {
        String[] commands = sess.getCommands();
        if (commands.length != 1
                && getCommandName(commands[0]).startsWith("exev_")) {
            if (keys.putIfAbsent(commands[1], commands[1]) == null) {
                int ret = execCommand0(commands);
                if (getCommandName(commands[0]).startsWith("exev_")) {
                    keys.remove(commands[1]);
                }
                return ret;
            } else {
                return -1;
            }
        } else { // e.g. balse
            return execCommand0(commands);
        }
    }

    private int execCommand0(String[] commands) throws Exception {
        String commandName = commands[0].toLowerCase();
        Command command = handler.getJavaCommandMap(commandName);
        Object ret;
        if (command != null) {
            ret = command.execute(this, commands);
        } else {
            ret = execCommand(commands);
        }
        return 0;
    }

    // public int execCommand(String[] commands) throws IOException {
    // return 0;
    // }

    public Object execCommand(String[] commands) throws IOException {
        String command = commands[0].toLowerCase();
        if (command.equals("set")) {
            execSetCommand(commands);
        } else if (command.equals("get")) {
            execGetCommand(commands);
        } else if (command.equals("balse")) {
            execBalseCommand(commands);
        } else {
            execErrorCommand(commands);
            // throw new RuntimeException("Command not found");
        }
        return null;
    }

    public void execSetCommand(String[] commands) throws IOException {
        // commands[0]: command
        // commands[1]: key
        // commands[2]: flag
        // commands[3]: expire time
        int len = Integer.parseInt(commands[4]); // len
        // System.out.println("len: " + len);
        byte[] bytes = readBytes(len);
        // System.out.println("val: " + new String(bytes));
        readBytes(2); // "\r\n"
        writeString("STORED\r\n");
    }

    public void execGetCommand(String[] commands) throws IOException {
        // TODO
    }

    public void execBalseCommand(String[] commands) throws IOException {
        writeString("Are you sure?(yes/no)\r\n");
        String s = gets();
        if (s.equals("yes\r\n")) {
            stopEventLoop();
        } else {
        }
    }

    public void execErrorCommand(String[] commands) throws IOException {
        writeString("Command not found: " + commands[0] + "\r\n");
    }

    public void closeSilently() {
        try {
            sess.close();
        } catch (IOException e) {
        }
    }
}