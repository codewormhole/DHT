import java.net.ServerSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.Closeable;

import java.util.Arrays;
import java.util.Random;

public class DHT {

    private final RemoteMember[] mPeers;
    private final LocalMember mSelf;
    private final RequestListener mServer;

    public static void main(String[] args) throws Exception  {
        DHT dht = new DHT(args[0], Arrays.copyOfRange(args, 1, args.length));
        Random rand = new Random();
        for(int i = 0; i < 10; i++) {
            dht.put(Integer.toString(rand.nextInt()), Integer.toString(rand.nextInt()));
        }
        Thread.currentThread().sleep(1000);
        dht.stats();
    }

    public DHT(String self, String[] endpoints) throws IOException {
        mSelf = new LocalMember();
        mServer = new RequestListener(self, mSelf);
        mPeers = new RemoteMember[endpoints.length];
        int i = 0;
        for(String endpoint : endpoints) {
            InetSocketAddress address = getAddressFromString(endpoint);
            while(true) {
                try {
                    Socket clientSocket = new Socket();
                    clientSocket.connect(address);
                    System.out.println("Connected to " + address + " successfully");
                    mPeers[i++] = new RemoteMember(clientSocket);
                    break;
                } catch(IOException e) {
                    System.out.println("Failed to connect to " + address);
                    try {
                        Thread.currentThread().sleep(1000);
                    } catch(InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }
            }
        }
    }

    public void put(String key, String val) throws IOException {
        mSelf.put(key.getBytes(), val.getBytes());
        for(RemoteMember r : mPeers) {
            r.put(key.getBytes(), val.getBytes());
        }
    }

    public void stats() {
        System.out.println("Size : " + mSelf.mDataStore.size());
    }
    public static InetSocketAddress getAddressFromString(String endpoint) {
        int indexOfColon = endpoint.indexOf(":");
        if(indexOfColon <= 0) {
            throw new IllegalArgumentException("Invalid endpoint");
        }
        return new InetSocketAddress(endpoint.substring(0, indexOfColon), Integer.valueOf(endpoint.substring(indexOfColon+1, endpoint.length())));
    }

    private static class RequestListener implements Closeable {
    
        private static final AtomicInteger COUNTER = new AtomicInteger();
        private final ExecutorService mExecutor;
        private final ServerSocket mServerSocket;
        private final Future<?> mConnectionAcceptor;
        private final LocalMember mLocalMember;
        
        public RequestListener(String localEndpoint, LocalMember localMember) throws IOException {
            mLocalMember = localMember;
            InetSocketAddress address = getAddressFromString(localEndpoint); 
            mServerSocket = new ServerSocket(address.getPort());
            System.out.println("Started server : " + mServerSocket);
            mExecutor = Executors.newCachedThreadPool(t -> { return new Thread(t, "RequestListener-" + COUNTER.incrementAndGet());});
            mConnectionAcceptor = mExecutor.submit(() -> {
                while(!mServerSocket.isClosed() && !Thread.currentThread().isInterrupted()) {
                    try {
                        Socket clientSocket = mServerSocket.accept();
                        mExecutor.submit(() -> { 
                            try {
                                handle(clientSocket);
                            } catch(IOException i) {
                                throw new RuntimeException(i);
                            }
                            });
                    } catch(IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                });
        }

        public void close() throws IOException {
            mServerSocket.close();
            mConnectionAcceptor.cancel(true);
            mExecutor.shutdownNow();
        }

        void handle(Socket s) throws IOException {
            InputStream si = s.getInputStream();
            OutputStream so = s.getOutputStream();
            byte[] data = new byte[1024];
            while(!s.isClosed() && !Thread.currentThread().isInterrupted()) {
                int readSize = si.read(data);
                if(readSize < 0) {
                    s.close();
                    return;
                }
                int operationId = ByteUtils.decodeInt(data, 0);
                switch(operationId) {
                    case 1 :
                        {
                        int keySize = ByteUtils.decodeInt(data, 4);
                        int valueSize = ByteUtils.decodeInt(data, 8);
                        byte[] key = new byte[keySize];
                        byte[] value = new byte[valueSize];
                        if(keySize + valueSize > readSize - 12) {
                            byte[] extraData = new byte[keySize + valueSize + 12 - readSize];
                            int totalData = 0;
                            while(totalData < extraData.length) {
                                int extraReadSize = si.read(extraData, totalData, extraData.length - totalData);
                                if(extraReadSize < 0) {
                                    throw new IOException("EOF reached too early");
                                }
                                totalData += extraReadSize;
                            }

                            if(keySize + 12 <= readSize) {
                                System.arraycopy(data, 12, key, 0, keySize);
                                if(keySize + 12 < readSize) {
                                    System.arraycopy(data, keySize + 12, value, 0, readSize - keySize - 12);
                                }
                                System.arraycopy(extraData, 0, value, readSize - keySize - 12, extraData.length);
                            } else {
                                System.arraycopy(data, 12, key, 0, readSize - 12);
                                System.arraycopy(extraData, 0, key, readSize - 12, keySize + 12 - readSize);
                                System.arraycopy(extraData, keySize + 12 - readSize, value, 0, valueSize);
                            }
                        } else {
                            System.arraycopy(data, 12, key, 0, keySize);
                            System.arraycopy(data, keySize + 12, value, 0, valueSize);
                        }
                        mLocalMember.put(key, value);
                        byte[] result = new byte[4];
                        ByteUtils.encodeInt(1, result, 0);
                        so.write(result);
                        so.flush();
                        break;
                        }
                    case 2 :
                        {
                        int keySize = ByteUtils.decodeInt(data, 4);
                        byte[] key = new byte[keySize];
                        if(keySize > readSize - 8) {
                            byte[] extraData = new byte[keySize + 8 - readSize];
                            int totalData = 0;
                            while(totalData < extraData.length) {
                                int extraReadSize = si.read(extraData, totalData, extraData.length - totalData);
                                if(extraReadSize < 0) {
                                    throw new IOException("EOF reached too early");
                                }
                                totalData += extraReadSize;
                            }
                            System.arraycopy(data, 8, key, 0, readSize - 8);
                            System.arraycopy(extraData, 0, key, readSize - 8, extraData.length);
                        } else {
                            System.arraycopy(data, 8, key, 0, keySize);
                        }
                        byte[] value = mLocalMember.get(key);
                        byte[] resultHeader = new byte[8];
                        ByteUtils.encodeInt(1, resultHeader, 0);
                        ByteUtils.encodeInt(value.length, resultHeader, 4);
                        so.write(resultHeader);
                        so.write(value);
                        so.flush();
                        break;
                        }
                    default :
                        System.out.println("Invalid operation");
                }
            }
        }
        
    }

    public static interface Member extends Closeable {
        public void put(byte[] key, byte[] value) throws IOException;
        public byte[] get(byte[] key) throws IOException;
    }

    public static class RemoteMember implements Member {

        private final Socket mRemoteConnection;
        private final InputStream mInputStream;
        private final OutputStream mOutputStream;

        public RemoteMember(Socket connection) throws IOException {
            mRemoteConnection = connection;
            mInputStream = mRemoteConnection.getInputStream();
            mOutputStream = mRemoteConnection.getOutputStream();
        }
        
        public void close() throws IOException {
            mRemoteConnection.close();
        }

        public void put(byte[] key, byte[] val) throws IOException {
            byte[] requestHeader = new byte[12];
            ByteUtils.encodeInt(1, requestHeader, 0);
            ByteUtils.encodeInt(key.length, requestHeader, 4);
            ByteUtils.encodeInt(val.length, requestHeader, 8);
            mOutputStream.write(requestHeader);
            mOutputStream.write(key);
            mOutputStream.write(val);
            mOutputStream.flush();
            byte[] result = new byte[4];
            mInputStream.read(result);
            int resultInt = ByteUtils.decodeInt(result, 0);
            if(resultInt == 1) {
                return;
            } else {
                throw new IOException("Put failed with error code : " + resultInt);
            }
        }

        public byte[] get(byte[] key) throws IOException {
            byte[] requestHeader = new byte[8];
            ByteUtils.encodeInt(2, requestHeader, 0);
            ByteUtils.encodeInt(key.length, requestHeader, 4);
            mOutputStream.write(requestHeader);
            mOutputStream.write(key);
            mOutputStream.flush();
            byte[] resultHeader = new byte[8];
            mInputStream.read(resultHeader);
            int resultInt = ByteUtils.decodeInt(resultHeader, 0);
            if(resultInt != 1) {
                throw new IOException("Get failed with error code : " + resultInt);
            }
            int valueSize = ByteUtils.decodeInt(resultHeader, 4);
            byte[] value = new byte[valueSize];
            int totalRead = 0;
            while(totalRead < valueSize) {
                int bytesRead = mInputStream.read(value, totalRead, value.length - totalRead);
                if(bytesRead < 0) {
                    throw new IOException("EOF reached while reading response");
                }
                totalRead += bytesRead;
            }
            return value;
        }
    }

    public static class LocalMember implements Member {
        
        private final Map<byte[], byte[]> mDataStore;
        public LocalMember() {
            mDataStore = new ConcurrentHashMap<>();
        }

        public void close() {

        }

        public void put(byte[] key, byte[] value) {
            mDataStore.put(key, value);
        }

        public byte[] get(byte[] key) {
            return mDataStore.get(key);
        }
    }
}
