package jp.co.rakuten.rit.roma.command.java;

import jp.co.rakuten.rit.roma.command.Command;
import jp.co.rakuten.rit.roma.command.Receiver;

public class SystemCommands {
    public static class QuitCommand extends Command {
        @Override
        public String getName() {
            return "quit";
        }

        @Override
        public int execute(Receiver receiver, String[] commands)
                throws Exception {
            receiver.getSession().close();
            return 1;
        }
    }
}