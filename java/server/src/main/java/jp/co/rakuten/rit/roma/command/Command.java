package jp.co.rakuten.rit.roma.command;

public class Command {

    public String getAliasName() {
        throw new UnsupportedOperationException();
    }

    public String getName() {
        throw new UnsupportedOperationException();
    }

    public Object execute(Receiver receiver, String[] commands)
            throws Exception {
        throw new UnsupportedOperationException();
    }
}