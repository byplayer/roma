package jp.co.rakuten.rit.roma.event;

import jp.co.rakuten.rit.roma.command.ConnectionFactory;
import jp.co.rakuten.rit.roma.command.ConnectionPoolFactory;
import jp.co.rakuten.rit.roma.command.ReceiverFactory;

public class Test {

    public static void main(String[] args) throws Exception {
        HandlerFactory handlerFactory = new HandlerFactory();
        HandlerBase handler = handlerFactory.newHandler("0.0.0.0", 11211,
                new ConnectionPoolFactory(),
                new ConnectionFactory());
        handler.addCommandMap("set", "exev_set");
        handler.run(new ReceiverFactory());
    }
}