package jp.co.rakuten.rit.roma.routing;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.arnx.jsonic.JSON;

public class RoutingData {

    public static RoutingData create(String rawData) {
        List<Object> jsonObject = (List<Object>) JSON.decode(rawData);
        RoutingData routingData = new RoutingData();

        Map<String, Object> map = (Map<String, Object>) jsonObject.get(0);
        routingData.dgstBits = ((BigDecimal) map.get("dgst_bits")).intValue();
        routingData.divBits = ((BigDecimal) map.get("div_bits")).intValue();
        routingData.redundantNumber = ((BigDecimal) map.get("rn")).intValue();
        // this.hBits = new BigInteger("2").pow(dgstBits);
        routingData.maskBits = routingData.dgstBits - routingData.divBits;
        routingData.nodeIDs = toNodeIDs((List<String>) jsonObject.get(1));
        routingData.virtualNodeIndexes = toVirtualNodeIndexes((Map<String, List<String>>) jsonObject
                .get(2));
        routingData.virtualNodeIDs = toVirtualNodeIDs(routingData.virtualNodeIndexes);
        routingData.virtualNodeClocks = toVirtualNodeClocks((Map<String, BigDecimal>) jsonObject
                .get(3));
        return routingData;
    }

    private static List<String> toNodeIDs(List<String> nodeIDs) {
        Object[] o = (Object[]) nodeIDs.toArray();
        nodeIDs.clear();
        Arrays.sort(o);
        for (int i = 0; i < o.length; ++i) {
            nodeIDs.add((String) o[i]);
        }
        return nodeIDs;
    }

    private static Map<Long, List<String>> toVirtualNodeIndexes(
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

    private static List<Long> toVirtualNodeIDs(
            Map<Long, List<String>> virtualNodeIndexes) {
        Long[] o = new Long[virtualNodeIndexes.size()];
        Iterator<Long> nids = virtualNodeIndexes.keySet().iterator();
        for (int i = 0; i < o.length; ++i) {
            o[i] = nids.next();
        }
        Arrays.sort(o);
        List<Long> ret = new ArrayList<Long>();
        for (int i = 0; i < o.length; ++i) {
            ret.add(o[i]);
        }
        return ret;
    }

    private static Map<Long, Long> toVirtualNodeClocks(
            Map<String, BigDecimal> rawData) {
        Map<Long, Long> ret = new HashMap<Long, Long>();
        Iterator<String> vns = rawData.keySet().iterator();
        while (vns.hasNext()) {
            String vn = vns.next();
            long i = ((BigDecimal) rawData.get(vn)).longValue();
            ret.put(Long.parseLong(vn), i);
        }
        return ret;
    }

    private int dgstBits; // uint8

    private int divBits; // uint8

    private int redundantNumber; // uint8

    // the range of hash values is to 2**dgstBits from zero
    // protected BigInteger hBits; // default 32

    // search_mask = 2**(divBits - 1) << (dgstBits - divBits)
    private int maskBits; // uint8

    private List<String> nodeIDs;

    private List<Long> virtualNodeIDs;

    private Map<Long, List<String>> virtualNodeIndexes;

    private Map<Long, Long> virtualNodeClocks;

    RoutingData() {
    }

    public int getDgstBits() {
        return dgstBits;
    }
    
    public int getDivBits() {
        return divBits;
    }
    
    public int getRedundantNumber() {
        return redundantNumber;
    }
    
    public int getMaskBits() {
        return maskBits;
    }
    
    public List<String> getNodeIDs() {
        return nodeIDs;
    }
    
    public void setNodeIDs(List<String> nodeIDs) {
        this.nodeIDs = nodeIDs;
    }
    
    public List<Long> getVirtuanlNodeIDs() {
        return virtualNodeIDs;
    }
    
    public void setVirtualNodeIDs(List<Long> vnIDs) {
        virtualNodeIDs = vnIDs;
    }
    
    public Map<Long, List<String>> getVirtualNodeIndexes() {
        return virtualNodeIndexes;
    }
    
    public Map<Long, Long> getVirtualNodeClocks() {
        return virtualNodeClocks;
    }
}