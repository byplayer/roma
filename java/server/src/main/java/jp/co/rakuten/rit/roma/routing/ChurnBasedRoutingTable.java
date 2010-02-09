package jp.co.rakuten.rit.roma.routing;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChurnBasedRoutingTable extends RoutingTable {

    private static final Logger LOG =
        LoggerFactory.getLogger(ChurnBasedRoutingTable.class);

    protected String fileName;
    
    protected boolean enabledFailOver;
    
    protected FileWriter logWriter;
    
    protected Map<String, Integer> versions;
    
    protected int smallVersion = -1;

    public ChurnBasedRoutingTable(String data, String fileName) {
        super(data);
        this.fileName = fileName;
        enabledFailOver = false;
        logWriter = getLogWriter(fileName);
        this.versions = new HashMap<String, Integer>();
        // TODO
    }
    
    public FileWriter getLogWriter(String fileName) {
        return null;
    }
    
    public void getStats() {
        // TODO
    }
    
    public void setVersion(String nodeID, int version) {
        versions.put(nodeID, version);
        if (version == -1 || smallVersion > version) {
            smallVersion = version;
        }
    }
}
