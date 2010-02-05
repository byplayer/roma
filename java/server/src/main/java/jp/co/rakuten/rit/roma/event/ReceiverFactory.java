package jp.co.rakuten.rit.roma.event;

public class ReceiverFactory {

    public Receiver newReceiver(AbstractHandler handler, Session sess) {
        preReceiverInit(handler, sess);
        Receiver receiver = initReceiver(handler, sess);
        postReceiverInit(receiver);
        return receiver;
    }

    public void preReceiverInit(AbstractHandler handler, Session sess) {
    }

    public Receiver initReceiver(AbstractHandler handler, Session sess) {
        return new Receiver(handler, sess);
    }

    public void postReceiverInit(Receiver receiver) {
    }
}
