package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandlerImpl extends HandlerBase {
    private static final Logger LOG =
        LoggerFactory.getLogger(HandlerImpl.class);

    class CommandTaskImpl implements Runnable {

        public CommandTaskImpl() {
        }

        public void run() {
            while (true) {
                Receiver receiver = null;
                try {
                    receiver = eventQueue.take();
                    int ecode = receiver.execCommand();
                    if (ecode < 0) {
                        try {
                            eventQueue.put(receiver);
                            continue;
                        } catch (InterruptedException e) {
                        }
                    }
                    SocketChannel ch = receiver.getSession().getSocketChannel();
                    synchronized (reregisterChannels) {
                        for (Iterator<ReregisterChannel> i = reregisterChannels
                                .iterator(); i.hasNext();) {
                            ReregisterChannel rc = i.next();
                            if (rc.channel.equals(ch)) {
                                rc.registerable = true;
                                break;
                            }
                        }
                    }
                    selector.wakeup();
                } catch (InterruptedException e) {
                    if (receiver != null) {
                        receiver.closeSilently();
                    }
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private Selector selector;

    private int eventExecutorNumber;

    private ExecutorService eventExecutor;

    BlockingQueue<Receiver> eventQueue;

    private List<ReregisterChannel> reregisterChannels;

    private static Map<String, Receiver> receiverPool;

    public HandlerImpl(final String hostName, final int port)
            throws IOException {
        initHandler(hostName, port);
    }

    @Override
    public void initHandler(String hostName, int port) throws IOException {
        super.initHandler(hostName, port);
        serverSocketChannel.configureBlocking(false);
        selector = Selector.open();
        eventExecutorNumber = 100;
        eventQueue = new LinkedBlockingQueue<Receiver>();
        // eventQueue = new ArrayBlockingQueue<Receiver>(100);
        eventExecutor = Executors.newFixedThreadPool(eventExecutorNumber);
        // commandProcessorExecutor = Executors.newCachedThreadPool();
        reregisterChannels = new LinkedList<ReregisterChannel>();
        receiverPool = Collections
                .synchronizedMap(new HashMap<String, Receiver>());
    }

    @Override
    public void startHandler() throws IOException {
        for (int i = 0; i < eventExecutorNumber; ++i) {
            eventExecutor.execute(new CommandTaskImpl());
        }
        super.startHandler();
    }

    @Override
    public void stopHandler() {
        super.stopHandler();
        if (eventExecutor != null && !eventExecutor.isShutdown()) {
            eventExecutor.shutdownNow();
            if (!eventExecutor.isTerminated()) {
                eventExecutor.shutdown();
            }
        }
    }

    @Override
    public void startService() throws IOException {
        super.startService();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        try {
            startEventLoop();
        } catch (Exception e) {
            LOG.error("001", e);
        } finally {
            close();
        }
    }

    private void startEventLoop() throws IOException {
        while (enabledEventLoop) {
            // selector.select(selectInterval);
            // selector.selectNow();
            selector.select();
            reregisterChannels();

            for (Iterator<SelectionKey> keys = selector.selectedKeys()
                    .iterator(); keys.hasNext();) {
                SelectionKey key = keys.next();
                keys.remove();

                if (!key.isValid()) {
                    continue;
                }
                if (key.isValid() && key.isAcceptable()) {
                    handleAcceptable(key);
                }
                if (key.isValid() && key.isReadable()) {
                    handleReadable(key);
                }
                if (key.isValid() && key.isConnectable()) {
                    // ignore
                }
                if (key.isValid() && key.isWritable()) {
                    // ignore
                }
            }
        }
    }

    @Override
    public void stopService() {
        super.stopService();
    }

    private void reregisterChannels() {
        synchronized (reregisterChannels) {
            Iterator<ReregisterChannel> iter = reregisterChannels.iterator();
            for (; iter.hasNext();) {
                ReregisterChannel rc = iter.next();
                try {
                    if (rc.registerable) {
                        rc.channel.configureBlocking(false);
                        rc.channel.register(selector, SelectionKey.OP_READ);
                        iter.remove();
                    }
                } catch (IOException e) {
                    // e.printStackTrace();
                }
            }
        }
    }

    private void handleAcceptable(SelectionKey key) throws IOException {
        ServerSocketChannel ssock = (ServerSocketChannel) key.channel();
        SocketChannel channel = null;
        try {
            channel = ssock.accept();
            if (channel == null) {
                return;
            }
            channel.configureBlocking(false);
            channel.register(key.selector(), SelectionKey.OP_READ);
            createReceiver(channel);
            LOG.info("open connection: " + channel);
        } catch (ClosedChannelException e) {
            LOG.error("002", e);
            key.cancel();
            throw e;
        } catch (IOException e) {
            LOG.error("003", e);
            if (channel != null) {
                Receiver receiver = removeReceiver(channel);
                if (receiver != null) {
                    receiver.closeSilently();
                }
            }
        }
    }

    private void handleReadable(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        Receiver receiver = getReceiver(channel);
        String commandLine = null;
        try {
            commandLine = receiver.readLine();
        } catch (IOException e) {
            if (receiver != null) {
                receiver.closeSilently();
            }
        }
        if (commandLine != null) {
            receiver.setCommands(commandLine);
            // System.out.println("# cl: " + commandLine);
            // System.out.println("# key: " + key);
            // System.out.println("# channel: " + key.channel());
            try {
                key.cancel();
                synchronized (reregisterChannels) {
                    reregisterChannels.add(new ReregisterChannel(channel));
                }
                channel.configureBlocking(true);
                eventQueue.put(receiver);
            } catch (InterruptedException e) {
                e.printStackTrace();
                LOG.error("004", e);
            }
        }
    }

    class ReregisterChannel {
        public boolean registerable;

        public SocketChannel channel;

        public ReregisterChannel(SocketChannel channel) {
            this.channel = channel;
            this.registerable = false;
        }
    }

    @Override
    public void close() {
        try {
            if (selector != null) {
                selector.close();
            }
            selector = null;
        } catch (IOException e) {
        }
        super.close();
    }

    Receiver createReceiver(SocketChannel channel) {
        Receiver receiver = receiverPool.get(channel.toString());
        if (receiver != null) {
            throw new IllegalStateException();
        }
        Session sess = new Session(channel);
        receiver = receiverFactory.newReceiver(this, sess);
        // receiver = new Receiver(sess);
        receiverPool.put(channel.toString(), receiver);
        return receiver;
    }

    Receiver getReceiver(SocketChannel channel) {
        Receiver receiver = receiverPool.get(channel.toString());
        if (receiver == null) {
            throw new IllegalStateException("Receiver not found: " + channel);
        }
        return receiver;
    }

    Receiver removeReceiver(SocketChannel channel) {
        Receiver receiver = receiverPool.remove(channel.toString());
        if (receiver == null) {
            throw new IllegalStateException("Receiver not found: " + channel);
        }
        return receiver;
    }
}
