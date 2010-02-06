package jp.co.rakuten.rit.roma.routing;

import net.arnx.jsonic.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingTable {

    private static final Logger LOG = LoggerFactory
            .getLogger(RoutingTable.class);

    protected Object routingData;

    protected int maskBits;

    protected int failCount;

    protected int failCountThreshold;

    protected int failCountablePeriod;

    protected Object markleHashTree;

    protected int hBits;

    protected int redundantNum;

    protected int divBits;

    public RoutingTable(String data, String fileName) {
        System.out.println("fileName: " + fileName);
        initRoutingData(data);
        // TODO
        makeMarkleHashTree();
    }

    private void initRoutingData(String data) {
        Object routingData = JSON.decode(data);
        System.out.println("routing data: " + routingData);
    }

    private void makeMarkleHashTree() {
    }

    public String[] getNodeIDs() {
        return null;
    }

    public String[] getVirtualNodeIDs() {
        return null;
    }

    public int getVirtualNodeID(long hash) {
        return -1;
    }

    public String[] searchNodes(int virtualNodeID) {
        return null;
    }

    public void leave(String nodeID) {
    }

    public void procFailure() {

    }

    public void procSuccess() {

    }
}
