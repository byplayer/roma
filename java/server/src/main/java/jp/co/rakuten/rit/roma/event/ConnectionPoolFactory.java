package jp.co.rakuten.rit.roma.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionPoolFactory {
    private static Logger LOG =
        LoggerFactory.getLogger(ConnectionPoolFactory.class);

    public ConnectionPool newConnectionPool(int size) {
        preConnectionPoolInit();
        ConnectionPool connPool = initConnectionPool(size);
        postConnectionPoolInit(connPool);
        return connPool;
    }
    
    public void preConnectionPoolInit() {
    }
    
    public ConnectionPool initConnectionPool(int size) {
        return new ConnectionPool(size);
    }
    
    public void postConnectionPoolInit(ConnectionPool connPool) {
    }
}