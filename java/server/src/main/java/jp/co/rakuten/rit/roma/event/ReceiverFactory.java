package jp.co.rakuten.rit.roma.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiverFactory {
    private static final Logger LOG =
        LoggerFactory.getLogger(ReceiverFactory.class);

    public Receiver newReceiver(HandlerBase handler, Session sess) {
        preReceiverInit(handler, sess);
        Receiver receiver = initReceiver(handler, sess);
        postReceiverInit(receiver);
        return receiver;
    }

    public void preReceiverInit(HandlerBase handler, Session sess) {
    }

    public Receiver initReceiver(HandlerBase handler, Session sess) {
        return new Receiver(handler, sess);
    }

    public void postReceiverInit(Receiver receiver) {
    }
}
