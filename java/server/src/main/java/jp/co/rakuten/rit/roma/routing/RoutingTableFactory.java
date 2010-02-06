package jp.co.rakuten.rit.roma.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingTableFactory {

    private static final Logger LOG = LoggerFactory
            .getLogger(RoutingTableFactory.class);

    public RoutingTable newRoutingTable(String data, String fileName) {
        preRoutingTableInit();
        RoutingTable routingTable = initRoutingTable(data, fileName);
        postRoutingTableInit(routingTable);
        return routingTable;
    }

    public void preRoutingTableInit() {
    }

    public RoutingTable initRoutingTable(String data, String fileName) {
        System.out.println("$$$$: " + data);
        return new RoutingTable(data, fileName);
    }

    public void postRoutingTableInit(RoutingTable routingTable) {
    }
}
