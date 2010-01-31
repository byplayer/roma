package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import jp.co.rakuten.rit.roma.messaging.ConnectionPool;

public abstract class Handler {

    private static final Logger LOG =
	Logger.getLogger(Handler.class.getName());

    class ServiceImpl implements Runnable {

        public ServiceImpl() {
        }

        public void run() {
            try {
                startService();
            } catch (Exception e) {
                LOG.throwing(this.getClass().getName(), "run", e);
            } finally {
                ;
            }
        }
    }

    private static Handler instance = null;

    public static void run(final String hostName, final int port,
            final ReceiverFactory receiverFactory) throws IOException {
        if (instance != null) {
            throw new IllegalStateException("EventHandler is already run.");
        }
        instance = new HandlerImpl2();
        instance.initHandler(port, receiverFactory);
        instance.startHandler();

        do {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                LOG.throwing(Handler.class.getName(), "run", e);
            }
        } while (enabledEventLoop);
        instance.stopHandler();
    }

    private static final int DEFAULT_BUFFER_SIZE = 8192;

    protected ServerSocketChannel serverSocketChannel;

    protected ExecutorService connExecutor;

    protected static boolean enabledEventLoop;

    protected ReceiverFactory receiverFactory;

    protected ConnectionPool connPool;

    public void initHandler(int port, ReceiverFactory factory)
            throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
        // serverSocketChannel.socket().setReuseAddress(true);
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        connExecutor = Executors.newSingleThreadExecutor();
        this.receiverFactory = factory;
    }

    public void startHandler() throws IOException {
        connExecutor.execute(new ServiceImpl());
    }

    public void stopHandler() {
        if (connExecutor != null && !connExecutor.isShutdown()) {
            connExecutor.shutdownNow();
        }
    }

    public void startService() throws IOException {
        enabledEventLoop = true;
    }

    public void stopService() {
        enabledEventLoop = false;
    }

    public void close() {
        try {
            if (serverSocketChannel != null) {
                serverSocketChannel.close();
            }
            serverSocketChannel = null;
        } catch (IOException e) {
        }
    }

    public ConnectionPool getConnectionPool() {
        return connPool;
    }
}
