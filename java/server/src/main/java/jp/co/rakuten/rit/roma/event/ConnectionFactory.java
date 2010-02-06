package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionFactory {
    private static Logger LOG = LoggerFactory.getLogger(ConnectionFactory.class);

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
