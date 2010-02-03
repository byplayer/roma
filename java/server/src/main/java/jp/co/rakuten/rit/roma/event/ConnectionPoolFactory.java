package jp.co.rakuten.rit.roma.event;

public class ConnectionPoolFactory {

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