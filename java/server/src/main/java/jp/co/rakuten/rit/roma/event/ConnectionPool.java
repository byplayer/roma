package jp.co.rakuten.rit.roma.event;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.util.HashMap;
import java.util.NoSuchElementException;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * 
 */
public class ConnectionPool {

    protected int size;
    private HashMap<String, SocketPool> pool = new HashMap<String, SocketPool>();

    public ConnectionPool(final int size) {
        this.size = size;
    }

    public synchronized Connection create(String nodeID) throws IOException {
        String[] splited = nodeID.split("_");
        SocketPool spool = new SocketPool(splited[0], Integer
                .parseInt(splited[1]), size);
        pool.put(nodeID, spool);
        try {
            return new Connection(spool.get());
        } catch (NoSuchElementException e) {
            throw new IOException(e);
        } catch (IllegalStateException e) {
            throw new IOException(e);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public synchronized Connection get(String nodeID) throws IOException {
        SocketPool spool = pool.get(nodeID);
        if (spool == null) {
            return create(nodeID);
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

    static class SocketPool implements Closeable {

        private ObjectPool pool;

        public SocketPool(final String host, final int port, int max) {
            pool = new GenericObjectPool(new PoolableObjectFactory() {

                public void destroyObject(Object obj) throws Exception {
                    if (obj instanceof Socket) {
                        ((Socket) obj).close();
                    }
                }

                public boolean validateObject(Object obj) {
                    if (obj instanceof Socket) {
                        return ((Socket) obj).isConnected();
                    }
                    return false;
                }

                public Object makeObject() throws Exception {
                    return new Socket(host, port);
                }

                public void activateObject(Object obj) throws Exception {
                    // do nothing
                }

                public void passivateObject(Object obj) throws Exception {
                    // do nothing
                }
            }, max);
        }

        public Socket get() throws Exception, NoSuchElementException,
                IllegalStateException {
            return (Socket) pool.borrowObject();
        }

        public void put(Socket socket) throws Exception {
            pool.returnObject(socket);
        }

        public void close() throws IOException {
            try {
                pool.clear();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}