package jp.co.rakuten.rit.roma.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChurnBasedRoutingTable extends RoutingTable {

    private static final Logger LOG =
        LoggerFactory.getLogger(ChurnBasedRoutingTable.class);
            
    public ChurnBasedRoutingTable(String data, String fileName) {
        super(data, fileName);
        // TODO
    }
}
