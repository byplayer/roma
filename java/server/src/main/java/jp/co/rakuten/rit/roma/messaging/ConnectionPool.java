package jp.co.rakuten.rit.roma.messaging;

import java.io.IOException;

/**
 * 
 */
public interface ConnectionPool {

    public Connection get(String nodeID) throws IOException;

    public void put(String nodeID, Connection conn) throws IOException;

    public void delete(String nodeID);

    public void closeAll();

}
