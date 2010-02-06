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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ConnectionPool {
    private static Logger LOG = LoggerFactory.getLogger(ConnectionPool.class);

    protected int size;

    private HashMap<String, CPool> cpool = new HashMap<String, CPool>();
    
    private ConnectionFactory connFactory = null;

    public ConnectionPool(final int size) {
        this.size = size;
    }

    public void setConnectionFactory(ConnectionFactory connFactory) {
        this.connFactory = connFactory;
    }

    public ConnectionFactory getConnectionFactory() {
        return connFactory;
    }

    public synchronized Connection create(String nodeID) throws IOException {
        String[] s = nodeID.split("_");
        CPool cp = new CPool(s[0], Integer.parseInt(s[1]), size);
                
        cpool.put(nodeID, cp);
        try {
            return cp.get();
        } catch (NoSuchElementException e) {
            throw new IOException(e);
        } catch (IllegalStateException e) {
            throw new IOException(e);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public synchronized Connection get(String nodeID) throws IOException {
        CPool cp = cpool.get(nodeID);
        if (cp == null) {
            return create(nodeID);
        }
        Connection conn = null;
        try {
            conn = cp.get();
        } catch (NoSuchElementException e) {
            throw new IOException(e);
        } catch (IllegalStateException e) {
            throw new IOException(e);
        } catch (ConnectException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
        return conn;
    }

    public synchronized void put(String nodeID, Connection conn)
            throws IOException {
        CPool cp = cpool.get(nodeID);
        try {
            cp.put(conn);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public synchronized void delete(String nodeID) {
        CPool cp = cpool.remove(nodeID);
        try {
            if (cp != null) {
                cp.close();
            }
        } catch (IOException e) { 
            // ignore
            // throw new IOException(e);
        }
    }

    public synchronized void closeAll() {
        for (CPool cp : cpool.values()) {
            try {
                cp.close();
            } catch (Exception e) {
                // ignore
                // throw new IOException(e);
            }
        }
        cpool.clear();
    }

    class CPool implements Closeable {

        private ObjectPool opool;

        public CPool(final String host, final int port, int max) {
            opool = new GenericObjectPool(new PoolableObjectFactory() {
                public void destroyObject(Object obj) throws Exception {
                    if (obj instanceof Connection) {
                        ((Connection) obj).close();
                    }
                }

                public boolean validateObject(Object obj) {
                    if (obj instanceof Connection) {
                        return ((Connection) obj).sock.isConnected();
                    }
                    return false;
                }

                public Object makeObject() throws Exception {
                    Socket sock = new Socket(host, port);
                    return connFactory.newConnection(sock);
                }

                public void activateObject(Object obj) throws Exception {
                    // ignore
                }

                public void passivateObject(Object obj) throws Exception {
                    // ignore
                }
            }, max);
        }

        public Connection get() throws Exception, NoSuchElementException,
                IllegalStateException {
            return (Connection) opool.borrowObject();
        }

        public void put(Connection conn) throws Exception {
            opool.returnObject(conn);
        }

        public void close() throws IOException {
            try {
                opool.clear();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}