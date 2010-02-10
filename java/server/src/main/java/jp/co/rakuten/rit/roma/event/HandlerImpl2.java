package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jp.co.rakuten.rit.roma.command.ConnectionFactory;
import jp.co.rakuten.rit.roma.command.ConnectionPoolFactory;
import jp.co.rakuten.rit.roma.command.Receiver;
import jp.co.rakuten.rit.roma.command.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerImpl2 extends HandlerBase {

    private static Logger LOG =
        LoggerFactory.getLogger(HandlerImpl2.class);

    class ReceiverTaskImpl implements Runnable {
        private Receiver receiver;

        public ReceiverTaskImpl(Receiver receiver) {
            this.receiver = receiver;
        }

        public void run() {
            while (true) {
                try {
                    String commandLine = receiver.blockingReadLine();
                    // System.out.println("command line: " + commandLine);
                    receiver.setCommands(commandLine);
                    receiver.execCommand();
                } catch (Exception e) {
                    LOG.error("001", e);
                }
            }
        }
    }

    private int receiverExecutorNumber;

    private ExecutorService receiverExecutor;

    public HandlerImpl2(final String hostName, final int port,
            final ConnectionPoolFactory connPoolFactory,
            final ConnectionFactory connFactory)
            throws IOException {
        this.hostName = hostName;
        this.port = port;
        initConnectionPool(connPoolFactory, connFactory);
    }

    @Override
    public void init() throws IOException {
        super.init();
        this.receiverExecutorNumber = 100;
        receiverExecutor = Executors.newFixedThreadPool(receiverExecutorNumber);
        // receiverExecutor = Executors.newCachedThreadPool();
    }

    @Override
    public void start() throws IOException {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        if (receiverExecutor != null && !receiverExecutor.isShutdown()) {
            receiverExecutor.shutdownNow();
        }
    }

    @Override
    public void startService() throws IOException {
        super.startService();
        serverSocketChannel.configureBlocking(true);
        try {
            startEventLoop();
        } catch (Exception e) {
            LOG.error("002", e);
        } finally {
            close();
        }
    }

    private void startEventLoop() throws IOException {
        while (enabledEventLoop) {
            SocketChannel channel = null;
            try {
                channel = serverSocketChannel.accept();
                if (channel == null) {
                    continue;
                }
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error("003", e);
                return;
            }
            Receiver receiver = createReceiver(channel);
            LOG.info("open connection: " + channel);
            receiverExecutor.execute(new ReceiverTaskImpl(receiver));
        }
    }

    @Override
    public void stopService() {
        super.stopService();
        System.out.println("#1");
        // close();
        System.out.println("#2");
    }

    Receiver createReceiver(SocketChannel channel) {
        Session sess = new Session(channel);
        Receiver receiver = receiverFactory.newReceiver(this, sess);
        return receiver;
    }
}
