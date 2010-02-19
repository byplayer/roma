package jp.co.rakuten.rit.roma.routing;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.arnx.jsonic.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingTable {

    private static final Logger LOG = LoggerFactory
            .getLogger(RoutingTable.class);

    protected List<Object> routingData;

    protected int dgstBits; // uint8

    protected int divBits; // uint8

    protected int redundantNum; // uint8

    // the range of hash values is to 2**dgstBits from zero
    // protected BigInteger hBits; // default 32

    // search_mask = 2**(divBits - 1) << (dgstBits - divBits)
    protected int maskBits; // uint8

    protected List<String> nodeIDs;

    protected Map<Long, List<String>> virtualNodeIndexes;

    protected Map<String, Long> virtualNodeClocks;

    public RoutingTable(String data) {
        initRoutingData(data);
    }

    protected void initRoutingData(String data) {
        routingData = (List<Object>) JSON.decode(data);

        Map<String, Object> map = (Map<String, Object>) routingData.get(0);
        dgstBits = ((BigDecimal) map.get("dgst_bits")).intValue();
        divBits = ((BigDecimal) map.get("div_bits")).intValue();
        redundantNum = ((BigDecimal) map.get("rn")).intValue();
        // this.hBits = new BigInteger("2").pow(dgstBits);
        maskBits = dgstBits - divBits;
        nodeIDs = toNodeIDs((List<String>) routingData.get(1));
        virtualNodeIndexes = toVirtualNodeIndexes((Map<String, List<String>>) routingData
                .get(2));
        virtualNodeClocks = toVirtualNodeClocks((Map<String, BigDecimal>) routingData
                .get(3));
    }

    private List<String> toNodeIDs(List<String> nodeIDs) {
        Object[] o = (Object[]) nodeIDs.toArray();
        nodeIDs.clear();
        Arrays.sort(o);
        for (int i = 0; i < o.length; ++i) {
            nodeIDs.add((String) o[i]);
        }
        return nodeIDs;
    }

    private Map<Long, List<String>> toVirtualNodeIndexes(
            Map<String, List<String>> rawData) {
        Map<Long, List<String>> ret = new HashMap<Long, List<String>>();
        Iterator<String> vns = rawData.keySet().iterator();
        while (vns.hasNext()) {
            String str = vns.next();
            List<String> nodeIDs = rawData.get(str);
            ret.put(Long.parseLong(str), nodeIDs);
        }
        return ret;
    }

    private Map<String, Long> toVirtualNodeClocks(
            Map<String, BigDecimal> rawData) {
        Map<String, Long> ret = new HashMap<String, Long>();
        Iterator<String> vns = rawData.keySet().iterator();
        while (vns.hasNext()) {
            String vn = vns.next();
            long i = ((BigDecimal) rawData.get(vn)).longValue();
            ret.put(vn, i);
        }
        return ret;
    }

    public int getDgstBits() {
        return dgstBits;
    }

    public int getDivBits() {
        return divBits;
    }

    public int getRedundantNumber() {
        return redundantNum;
    }

    public List<String> getNodeIDs() {
        return nodeIDs;
    }

    public void setNodeIDs(List<String> nodeIDs) {
        this.nodeIDs = nodeIDs;
    }

    public Map<Long, List<String>> getVirtualNodeIndexes() {
        return virtualNodeIndexes;
    }

    public Set<Long> getVirtualNodeIDs() {
        return virtualNodeIndexes.keySet();
    }

    public long getVirtualNodeID(long hash) {
        return ((hash << (64 - dgstBits)) >>> (maskBits + 64 - dgstBits));
    }

    public Map<String, Long> getVirtualNodeClocks() {
        return virtualNodeClocks;
    }

    public List<String> searchNodeIDs(long virtualNodeID) {
        return virtualNodeIndexes.get(virtualNodeID);
    }
}
