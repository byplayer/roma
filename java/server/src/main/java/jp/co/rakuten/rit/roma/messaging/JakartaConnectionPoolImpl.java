package jp.co.rakuten.rit.roma.messaging;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * 
 */
public class JakartaConnectionPoolImpl implements ConnectionPool {

    protected int size;
    private HashMap<String, SocketPool> pool = new HashMap<String, SocketPool>();

    public JakartaConnectionPoolImpl(final int size) {
        this.size = size;
    }

    public synchronized Connection get(String nodeID) throws IOException {
        SocketPool spool = pool.get(nodeID);
        if (spool == null) {
            String[] splited = nodeID.split("_");
            spool = new SocketPool(splited[0], Integer.parseInt(splited[1]),
                    size);
            pool.put(nodeID, spool);
        }
        Socket socket = null;
        try {
            socket = spool.get();
        } catch (NoSuchElementException e) {
            throw new IOException(e);
        } catch (IllegalStateException e) {
            throw new IOException(e);
        } catch (ConnectException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
        return new Connection(socket);
    }

    public synchronized void put(String nodeID, Connection conn)
            throws IOException {
        SocketPool spool = pool.get(nodeID);
        try {
            spool.put(conn.sock);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public synchronized void delete(String nodeID) {
        SocketPool spool = pool.remove(nodeID);
        try {
            if (spool != null) {
                spool.close();
            }
        } catch (IOException e) { // ignore
            // throw new IOException(e);
        }
    }

    public synchronized void closeAll() {
        for (SocketPool spool : pool.values()) {
            try {
                spool.close();
            } catch (Exception e) { // ignore
                // throw new IOException(e);
            }
        }
        pool.clear();
    }
}
