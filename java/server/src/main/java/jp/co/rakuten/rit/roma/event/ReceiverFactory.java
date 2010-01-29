package jp.co.rakuten.rit.roma.event;

public class ReceiverFactory {

    public Receiver newReceiver(Handler handler, Session sess) {
        preReceiverInit(sess);
        Receiver receiver = initReceiver(sess);
        receiver.setHandler(handler);
        postReceiverInit(receiver);
        return receiver;
    }

    public void preReceiverInit(Session sess) {
    }

    public Receiver initReceiver(Session sess) {
        return new Receiver(sess);
    }

    public void postReceiverInit(Receiver receiver) {
    }
}
