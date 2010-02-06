package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HandlerBase {
    private static final Logger LOG =
        LoggerFactory.getLogger(HandlerBase.class);

    class ServiceImpl implements Runnable {

        public ServiceImpl() {
        }

        public void run() {
            try {
                startService();
            } catch (Exception e) {
                LOG.error("001", e);
            } finally {
                ;
            }
        }
    }

    private static final int DEFAULT_BUFFER_SIZE = 8192;

    protected ServerSocketChannel serverSocketChannel;

    protected ExecutorService connExecutor;

    protected boolean enabledEventLoop;

    protected ReceiverFactory receiverFactory;

    protected int connPoolSize = 5;

    private ConnectionPool connPool;

    private Map<String, String> commandMap = Collections
            .synchronizedMap(new HashMap<String, String>());

    public void run(ReceiverFactory recvFactory,
            ConnectionPoolFactory connPoolFactory, ConnectionFactory connFactory)
            throws IOException {
        connPool = connPoolFactory.newConnectionPool(connPoolSize);
        connPool.setConnectionFactory(connFactory);
        LOG.info("create connection pool: " + connPoolSize);
        connExecutor = Executors.newSingleThreadExecutor();
        this.receiverFactory = recvFactory;
        startHandler();
        do {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                LOG.error("002", e);
            }
        } while (enabledEventLoop);
        stopHandler();
    }

    public void initHandler(String hostName, int port) throws IOException {
        LOG.info("initialize Event Handler");
        serverSocketChannel = ServerSocketChannel.open();
        // serverSocketChannel.socket().setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
        // serverSocketChannel.socket().setReuseAddress(true);
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        LOG.info("bind port: " + port);
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

    public void addCommandMap(String aliasName, String methodName) {
        commandMap.put(aliasName, methodName);
    }

    public String getCommandMap(String aliasName) {
        return commandMap.get(aliasName);
    }

    public ConnectionPool getConnectionPool() {
        return connPool;
    }
}
