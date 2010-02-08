package jp.co.rakuten.rit.roma.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChurnBasedRoutingTable extends RoutingTable {

    private static final Logger LOG =
        LoggerFactory.getLogger(ChurnBasedRoutingTable.class);

    protected String fileName;

    public ChurnBasedRoutingTable(String data, String fileName) {
        super(data);
        this.fileName = fileName;
//        Object[] o = (Object[]) list.toArray();
//        list.clear();
//        Arrays.sort(o);
//        for (int i = 0; i < o.length; ++i) {
//            list.add((String)o[i]);
//        }
        // TODO
    }
}
