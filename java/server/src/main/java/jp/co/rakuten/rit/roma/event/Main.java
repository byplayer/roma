package jp.co.rakuten.rit.roma.event;

public class Main {

    public static void main(String[] args) throws Exception {
        Handler.run("0.0.0.0", 11211, new ReceiverFactory());
    }
}