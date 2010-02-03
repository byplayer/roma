package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class HandlerImpl2 extends Handler {

    private static Logger LOG = Logger.getLogger(HandlerImpl2.class.getName());

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
                } catch (IOException e) {
                    LOG.throwing(this.getClass().getName(), "run", e);
                }
            }
        }
    }

    private int receiverExecutorNumber;

    private ExecutorService receiverExecutor;

    public HandlerImpl2() throws IOException {
    }

    @Override
    public void initHandler(int port,
            ReceiverFactory receiverFactory,
            ConnectionPoolFactory connPoolFactory)
            throws IOException {
        super.initHandler(port, receiverFactory, connPoolFactory);
        this.receiverExecutorNumber = 100;
        receiverExecutor = Executors.newFixedThreadPool(receiverExecutorNumber);
        // receiverExecutor = Executors.newCachedThreadPool();
    }

    @Override
    public void startHandler() throws IOException {
        super.startHandler();
    }

    @Override
    public void stopHandler() {
        super.stopHandler();
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
            LOG.throwing(this.getClass().getName(), "startService", e);
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
                LOG.throwing(this.getClass().getName(), "startEventLoop", e);
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
