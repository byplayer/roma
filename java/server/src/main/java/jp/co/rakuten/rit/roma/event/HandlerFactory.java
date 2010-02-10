package jp.co.rakuten.rit.roma.event;

import java.io.IOException;

import jp.co.rakuten.rit.roma.command.ConnectionFactory;
import jp.co.rakuten.rit.roma.command.ConnectionPoolFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerFactory {
    private static final Logger LOG =
        LoggerFactory.getLogger(HandlerFactory.class);

    public HandlerBase newHandler(final String hostName, final int port,
            final ConnectionPoolFactory connPoolFactory,
            final ConnectionFactory connFactory)
            throws IOException {
        preHandlerInit();
        HandlerBase handler = initHandler(hostName, port,
                connPoolFactory, connFactory);
        postHandlerInit(handler);
        return handler;
    }

    public void preHandlerInit() {
    }

    public HandlerBase initHandler(final String hostName, final int port,
            final ConnectionPoolFactory connPoolFactory,
            final ConnectionFactory connFactory)
            throws IOException {
        return new HandlerImpl(hostName, port, connPoolFactory, connFactory);
    }

    public void postHandlerInit(HandlerBase handler) {
    }
}
