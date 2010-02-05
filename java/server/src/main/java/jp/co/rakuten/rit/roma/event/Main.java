package jp.co.rakuten.rit.roma.event;

public class Main {

    public static void main(String[] args) throws Exception {
        HandlerFactory handlerFactory = new HandlerFactory();
        AbstractHandler handler = handlerFactory.newHandler("0.0.0.0", 11211);
        handler.addCommandMap("set", "exev_set");
        handler.run(new ReceiverFactory(), new ConnectionPoolFactory(),
                new ConnectionFactory());
    }
}