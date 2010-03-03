package jp.co.rakuten.rit.roma.routing;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingTable {

    private static final Logger LOG = LoggerFactory
            .getLogger(RoutingTable.class);

    protected Object rawData;

    protected RoutingData routingData;

    private long searchMask;

    public RoutingTable(Object data) {
        rawData = data;
    }

    public void initRoutingData(String data) {
        routingData = RoutingData.create(data);
        setSearchMask();
        rawData = null;
    }

    public int getDgstBits() {
        return routingData.getDgstBits();
    }

    public int getDivBits() {
        return routingData.getDivBits();
    }

    private void setSearchMask() {
        // TODO
        int maskBits = routingData.getMaskBits();
        searchMask = (0xFFFFFFFF >>> maskBits) << maskBits;
    }

    private long getSearchMask() {
        return searchMask;
    }

    public int getRedundantNumber() {
        return routingData.getRedundantNumber();
    }

    public List<String> getNodeIDs() {
        return routingData.getNodeIDs();
        // List<String> ret = new ArrayList<String>();
        // Iterator<String> nids = routingData.getNodeIDs().iterator();
        // while (nids.hasNext()) {
        // ret.add(nids.next());
        // }
        // return ret;
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
        return hash & getSearchMask();
    }

    public Map<Long, Long> getVirtualNodeClocks() {
        return routingData.getVirtualNodeClocks();
    }

    public List<String> searchNodeIDs(long virtualNodeID) {
        return getVirtualNodeIndexes().get(virtualNodeID);
    }
}