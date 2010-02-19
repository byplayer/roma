package jp.co.rakuten.rit.roma.routing;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingTable {

    private static final Logger LOG = LoggerFactory
            .getLogger(RoutingTable.class);

    protected RoutingData routingData;

    public RoutingTable(Object data) {
        routingData = RoutingData.create((String) data);
    }

    public int getDgstBits() {
        return routingData.getDgstBits();
    }

    public int getDivBits() {
        return routingData.getDivBits();
    }

    public int getRedundantNumber() {
        return routingData.getRedundantNumber();
    }

    public List<String> getNodeIDs() {
        return routingData.getNodeIDs();
//        List<String> ret = new ArrayList<String>();
//        Iterator<String> nids = routingData.getNodeIDs().iterator();
//        while (nids.hasNext()) {
//            ret.add(nids.next());
//        }
//        return ret;
    }

    public void setNodeIDs(List<String> nodeIDs) {
        routingData.setNodeIDs(nodeIDs);
    }

    public Map<Long, List<String>> getVirtualNodeIndexes() {
        return routingData.getVirtualNodeIndexes();
    }

    public List<Long> getVirtualNodeIDs() {
        return routingData.getVirtuanlNodeIDs();
    }

    public long getVirtualNodeID(long hash) {
        int dgstBits = routingData.getDgstBits();
        int maskBits = routingData.getMaskBits();
        return ((hash << (64 - dgstBits)) >>> (maskBits + 64 - dgstBits));
    }

    public Map<Long, Long> getVirtualNodeClocks() {
        return routingData.getVirtualNodeClocks();
    }

    public List<String> searchNodeIDs(long virtualNodeID) {
        return getVirtualNodeIndexes().get(virtualNodeID);
    }
}