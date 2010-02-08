package jp.co.rakuten.rit.roma.routing;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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

    protected int dgstBits;

    protected int divBits;

    protected int redundantNum;

    // the range of hash values is to 2**dgstBits from zero
    // protected BigInteger hBits; // default 32

    // search_mask = 2**(divBits - 1) << (dgstBits - divBits)
    protected int maskBits;

    protected int upperBits;

    protected Map<String, Integer> failCount;

    protected int failCountThreshold;

    protected int failCountablePeriod;

    protected long failTime;

    protected List<String> nodeIDs;

    protected Map<String, List<String>> virtualNodeIndexes;

    protected Map<String, Integer> virtualNodeClocks;

    protected Object markleHashTree;

    public RoutingTable(String data) {
        initRoutingData(data);
        makeMarkleHashTree();
    }

    protected void initRoutingData(String data) {
        routingData = (List<Object>) JSON.decode(data);

        Map<String, Object> map = (Map<String, Object>) routingData.get(0);
        dgstBits = ((BigDecimal) map.get("dgst_bits")).intValue();
        divBits = ((BigDecimal) map.get("div_bits")).intValue();
        redundantNum = ((BigDecimal) map.get("rn")).intValue();
        // this.hBits = new BigInteger("2").pow(dgstBits);
        maskBits = dgstBits - divBits;
        upperBits = 64 - dgstBits;
        nodeIDs = (List<String>) routingData.get(1);
        virtualNodeIndexes = (Map<String, List<String>>) routingData.get(2);
        virtualNodeClocks = toVirtualNodeClocks((
                Map<String, BigDecimal>) routingData.get(3));
                
        failCount = new HashMap<String, Integer>();
        failCountThreshold = 5;
        failCountablePeriod = 0;
        failTime = new Date().getTime();
    }

    private Map<String, Integer> toVirtualNodeClocks(
            Map<String, BigDecimal> rowData) {
        Map<String, Integer> ret = new HashMap<String, Integer>();
        Iterator<String> vns = rowData.keySet().iterator();
        while (vns.hasNext()) {
            String vn = vns.next();
            int i = ((BigDecimal) rowData.get(vn)).intValue();
            ret.put(vn, i);
        }
        return ret;
    }

    private void makeMarkleHashTree() {
        // TODO
    }

    public void getStats() {
        // TODO
    }

    public List<String> getNodeIDs() {
        return nodeIDs;
    }

    public Set<String> getVirtualNodeIDs() {
        return virtualNodeIndexes.keySet();
    }

    public String getVirtualNodeID(long hash) {
        int i = (int) ((hash << upperBits) >>> (maskBits + upperBits));
        return new Integer(i).toString();
    }

    public List<String> searchNodeIDs(String virtualNodeID) {
        return virtualNodeIndexes.get(virtualNodeID);
    }

    public void leave(String nodeID) {
        Iterator<String> vnIDs = virtualNodeIndexes.keySet().iterator();
        while (vnIDs.hasNext()) {
            String vnID = vnIDs.next();
            List<String> nodeIDs = virtualNodeIndexes.get(vnID);
            nodeIDs.remove(nodeID);
            if (nodeIDs.size() == 0) {
                LOG.error("Vnode data is lost.(Vnode=" + vnID + ")");
            }
            makeMarkleHashTree(); // TODO
        }
        failCount.remove(nodeID);
    }

    public String dump() {
        List<Object> list = new ArrayList<Object>();
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("dgst_bits", dgstBits);
        map.put("div_bits", divBits);
        map.put("rn", redundantNum);
        list.add(map);
        list.add(nodeIDs);
        list.add(virtualNodeIndexes);
        list.add(virtualNodeClocks);
        return JSON.encode(list);
    }

    public void procFailure(String nodeID) {
        long t = new Date().getTime();
        if (t - failTime > failCountablePeriod) {
            Integer i = failCount.get(nodeID);
            if (i == null) {
                failCount.put(nodeID, 1);
            } else {
                failCount.put(nodeID, i++);
            }
            if (i >= failCountThreshold) {
                leave(nodeID);
            }
        }
        failTime = t;
    }

    public void procSuccess(String nodeID) {
        failCount.remove(nodeID);
    }

    public void createNodesFromVirtualNodeIndexes() {
        // TODO
    }

    public static void main(String[] args) {
        Map map = new HashMap();
        map.put("div_bits", 2);
        map.put("dgst_bits", 10);

        List<String> list = new ArrayList<String>();
        list.add("localhost_11213");
        list.add("localhost_11211");
        list.add("localhost_11212");
        list.add("localhost_11214");
        System.out.println("list: " + list);
        Object[] o = (Object[]) list.toArray();
        list.clear();
        Arrays.sort(o);
        for (int i = 0; i < o.length; ++i) {
            list.add((String)o[i]);
        }
        System.out.println("list: " + list);

        List list1 = new ArrayList();
        list1.add(map);
        list1.add(list);
        String s = JSON.encode(list1);
        System.out.println("s: " + s);
    }
}
