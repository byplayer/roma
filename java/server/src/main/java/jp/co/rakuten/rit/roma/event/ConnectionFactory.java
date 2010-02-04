package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.net.Socket;

public class ConnectionFactory {
    public Connection newConnection(Socket sock) throws IOException {
        preConnectionInit();
        Connection conn = initConnection(sock);
        postConnectionInit(conn);
        return conn;
    }

    public void preConnectionInit() {
    }

    public Connection initConnection(Socket sock) throws IOException {
        return new Connection(sock);
    }

    public void postConnectionInit(Connection conn) {
    }
}
