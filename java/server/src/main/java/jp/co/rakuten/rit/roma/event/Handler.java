package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;


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
            final ReceiverFactory receiverFactory, 
            final ConnectionPoolFactory connPoolFactory, 
            final ConnectionFactory connFactory) throws IOException {
        if (instance != null) {
            throw new IllegalStateException("EventHandler is already run.");
        }
        instance = new HandlerImpl();
        instance.initHandler(port, receiverFactory, 
                connPoolFactory, connFactory);
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
    
    protected int connPoolSize = 5;

    private static ConnectionPool connPool;

    public void initHandler(int port,
            ReceiverFactory recvFactory, 
            ConnectionPoolFactory connPoolFactory,
            ConnectionFactory connFactory)
            throws IOException {
        LOG.info("initialize Event Handler");
        serverSocketChannel = ServerSocketChannel.open();
        // serverSocketChannel.socket().setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
        // serverSocketChannel.socket().setReuseAddress(true);
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        LOG.info("bind port: " + port);
        connPool = connPoolFactory.newConnectionPool(connPoolSize);
        connPool.setConnectionFactory(connFactory);
        LOG.info("create connection pool: " + connPoolSize);
        connExecutor = Executors.newSingleThreadExecutor();
        this.receiverFactory = recvFactory;
    }

    public void startHandler() throws IOException {
        LOG.info("start Event Handler");
        connExecutor.execute(new ServiceImpl());
    }

    public void stopHandler() {
        if (connExecutor != null && !connExecutor.isShutdown()) {
            connExecutor.shutdownNow();
        }
        close();
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
    
    public static ConnectionPool getConnectionPool() {
        return connPool;
    }
}
