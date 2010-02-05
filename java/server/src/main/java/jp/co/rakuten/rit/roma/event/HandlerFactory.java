package jp.co.rakuten.rit.roma.event;

import java.io.IOException;

public class HandlerFactory {

    public AbstractHandler newHandler(final String hostName, final int port)
            throws IOException {
        preHandlerInit();
        AbstractHandler handler = initHandler(hostName, port);
        postHandlerInit(handler);
        return handler;
    }

    public void preHandlerInit() {
    }

    public AbstractHandler initHandler(final String hostName, final int port)
            throws IOException {
        return new HandlerImpl(hostName, port);
    }

    public void postHandlerInit(AbstractHandler handler) {
    }
}
