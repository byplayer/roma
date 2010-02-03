package jp.co.rakuten.rit.roma.event;

import java.io.IOException;


public class Receiver {

    private Handler handler;

    private Session sess;

    public Receiver(Session sess) {
        this.sess = sess;
    }

    public void postInit() {
    }

    void setHandler(Handler handler) {
        this.handler = handler;
    }

    public Session getSession() {
        return sess;
    }

    public void setCommands(String commandLine) {
        sess.setCommands(commandLine);
    }

    public Connection getConnection(String nodeID) throws IOException {
        return Handler.getConnectionPool().get(nodeID);
    }

    public void putConnection(String nodeID, Connection conn)
            throws IOException {
        Handler.getConnectionPool().put(nodeID, conn);
    }

    public void stopEventLoop() {
        handler.stopService();
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

    public void execCommand() throws IOException {
        String[] commands = sess.getCommands();
        execCommand(commands);
    }

    // public void execCommand(String[] commands) throws IOException {
    // }

    public void execCommand(String[] commands) throws IOException {
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