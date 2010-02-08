package jp.co.rakuten.rit.roma.command.java;

import jp.co.rakuten.rit.roma.command.Command;
import jp.co.rakuten.rit.roma.command.Receiver;

public class SystemCommands {

    public static class BalseCommand extends Command {
        @Override
        public int execute(Receiver receiver, String[] commands)
                throws Exception {
            return -1;
        }
    }

    public static class RBalseCommand extends Command {
        @Override
        public int execute(Receiver receiver, String[] commands)
                throws Exception {
            return -1;
        }
    }

    public static class VersionCommand extends Command {
        @Override
        public int execute(Receiver receiver, String[] commands)
                throws Exception {
            return -1;
        }
    }

    public static class QuitCommand extends Command {
        @Override
        public int execute(Receiver receiver, String[] commands)
                throws Exception {
            receiver.getSession().close();
            return 1;
        }
    }

    public static class WhoamiCommand extends Command {
        @Override
        public int execute(Receiver receiver, String[] commands)
                throws Exception {
            return 1;
        }
    }

    public static class StatsCommand extends Command {
        @Override
        public int execute(Receiver receiver, String[] commands)
                throws Exception {
            return 1;
        }
    }

    public static class WriteBehindRotateCommand extends Command {
        @Override
        public int execute(Receiver receiver, String[] commands)
                throws Exception {
            return 1;
        }
    }

    public static class RWriteBehindRotateCommand extends Command {
        @Override
        public int execute(Receiver receiver, String[] commands)
                throws Exception {
            return 1;
        }
    }

}