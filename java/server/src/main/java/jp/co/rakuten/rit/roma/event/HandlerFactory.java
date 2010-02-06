package jp.co.rakuten.rit.roma.event;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerFactory {
    private static final Logger LOG =
        LoggerFactory.getLogger(HandlerFactory.class);

    public HandlerBase newHandler(final String hostName, final int port)
            throws IOException {
        preHandlerInit();
        HandlerBase handler = initHandler(hostName, port);
        postHandlerInit(handler);
        return handler;
    }

    public void preHandlerInit() {
    }

    public HandlerBase initHandler(final String hostName, final int port)
            throws IOException {
        return new HandlerImpl(hostName, port);
    }

    public void postHandlerInit(HandlerBase handler) {
    }
}
