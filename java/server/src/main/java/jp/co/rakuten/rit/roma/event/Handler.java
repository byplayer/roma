package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.net.InetSocketAddress;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class Handler {

	private static final Logger LOG = Logger.getLogger(Handler.class.getName());

	private static Handler instance = null;

	public static void run(final String hostName, final int port,
			final ReceiverFactory receiverFactory) throws IOException {
		if (instance != null) {
			throw new IllegalStateException("EventHandler is already run.");
		}
		instance = new Handler();
		instance.initHandler(port, receiverFactory);
		instance.startHandler();

		for (;;) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	class ConnTaskImpl implements Runnable {

		public ConnTaskImpl() {
		}

		public void run() {
			try {
				startEventService();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				;
			}
		}
	}

	class CommandTaskImpl implements Runnable {

		public CommandTaskImpl() {
		}

		public void run() {
			while (true) {
				try {
					Receiver receiver = eventQueue.take();
					receiver.execCommand();
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
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private final int DEFAULT_BUFFER_SIZE = 8192;

	private ServerSocketChannel serverSocketChannel;

	private Selector selector;

	private ExecutorService connExecutor;

	private int eventExecutorNumber;

	private ExecutorService eventExecutor;

	BlockingQueue<Receiver> eventQueue;

	private boolean enabledEventLoop;

	private List<ReregisterChannel> reregisterChannels;

	private static Map<String, Receiver> receiverPool;

	private ReceiverFactory receiverFactory;

	public Handler() throws IOException {
	}

	public void initHandler(int port, ReceiverFactory receiverFactory)
			throws IOException {
		selector = Selector.open();
		serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.configureBlocking(false);
		serverSocketChannel.socket().setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
		serverSocketChannel.socket().bind(new InetSocketAddress(port));
		eventExecutorNumber = 100;
		connExecutor = Executors.newSingleThreadExecutor();
		eventQueue = new LinkedBlockingQueue<Receiver>();
		// eventQueue = new ArrayBlockingQueue<Receiver>(100);
		eventExecutor = Executors.newFixedThreadPool(eventExecutorNumber);
		// commandProcessorExecutor = Executors.newCachedThreadPool();
		reregisterChannels = new LinkedList<ReregisterChannel>();
		receiverPool = Collections
				.synchronizedMap(new HashMap<String, Receiver>());
		this.receiverFactory = receiverFactory;
	}

	public void startHandler() throws IOException {
		for (int i = 0; i < eventExecutorNumber; ++i) {
			eventExecutor.execute(new CommandTaskImpl());
		}
		connExecutor.execute(new ConnTaskImpl());
	}

	public void stopHandler() {
		if (connExecutor != null && !connExecutor.isShutdown()) {
			connExecutor.shutdownNow();
		}
		if (eventExecutor != null && !eventExecutor.isShutdown()) {
			eventExecutor.shutdownNow();
		}
	}

	public void startEventService() throws IOException {
		serverSocketChannel.configureBlocking(false);
		serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
		try {
			enabledEventLoop = true;
			startEventLoop();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}

	public void stopEventService() {
		System.out.println("#### 0");
		enabledEventLoop = false;
	}

	private void startEventLoop() throws IOException {
		while (enabledEventLoop) {
			System.out.println("#### 1");
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
		System.out.println("#### 2");
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
			e.printStackTrace();
			LOG.throwing(this.getClass().getName(), "handleAcceptable", e);
			key.cancel();
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			LOG.throwing(this.getClass().getName(), "handleAcceptable", e);
			if (channel != null) {
				Receiver receiver = removeReceiver(channel);
				if (receiver != null) {
					receiver.getSession().close();
				}
			}
		}
	}

	private void handleReadable(SelectionKey key) throws IOException {
		SocketChannel channel = (SocketChannel) key.channel();
		Receiver receiver = getReceiver(channel);
		String commandLine = receiver.readLine();
		if (commandLine != null) {
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
				LOG.throwing(this.getClass().getName(), "handleReadable", e);
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

	private void close() {
		try {
			if (selector != null) {
				selector.close();
			}
			selector = null;
		} catch (IOException e) {
		}

		try {
			if (serverSocketChannel != null) {
				serverSocketChannel.close();
			}
			serverSocketChannel = null;
		} catch (IOException e) {
		}
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
