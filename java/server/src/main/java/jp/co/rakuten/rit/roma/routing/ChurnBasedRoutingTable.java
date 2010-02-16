package jp.co.rakuten.rit.roma.routing;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChurnBasedRoutingTable extends RoutingTable {

    private static final Logger LOG =
        LoggerFactory.getLogger(ChurnBasedRoutingTable.class);

    protected String fileName;
    
    protected boolean enabledFailOver;
    
    protected FileWriter logWriter;
    
    protected Map<String, Integer> versions; // the type of ver. is uint16
    
    protected int minVersion = -1;

    public ChurnBasedRoutingTable(String data, String fileName) {
        super(data);
        this.fileName = fileName;
        logWriter = getLogFileWriter(fileName);
        enabledFailOver = false;
        this.versions = new HashMap<String, Integer>();
        // TODO
    }
    
    public FileWriter getLogFileWriter(String fileName) {
        return null;
    }
    
    public void getStats() {
        // TODO
    }
    
    public void setVersion(String nodeID, int version) {
        versions.put(nodeID, version);
        if (version == -1 || minVersion > version) {
            minVersion = version;
        }
    }
    
    public int findMinVersion() {
        int ret = 0x0000ffff;
        Iterator<String> nodeIDs = versions.keySet().iterator();
        while (nodeIDs.hasNext()) {
            String nodeID = nodeIDs.next();
            int ver = versions.get(nodeID);
            if (ret > ver) {
                ret = ver;
            }
        }
        return ret;
    }
    
    public void doLeaveProcess() {
    }
    
    public void doLostProcess() {
    }
    
    
}
