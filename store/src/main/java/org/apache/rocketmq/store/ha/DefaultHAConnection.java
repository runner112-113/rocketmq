/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class DefaultHAConnection implements HAConnection {

    /**
     * Transfer Header buffer size. Schema: physic offset and body size. Format:
     *
     * <pre>
     * ┌───────────────────────────────────────────────┬───────────────────────┐
     * │                  physicOffset                 │         bodySize      │
     * │                    (8bytes)                   │         (4bytes)      │
     * ├───────────────────────────────────────────────┴───────────────────────┤
     * │                                                                       │
     * │                           Transfer Header                             │
     * </pre>
     * <p>
     */
    public static final int TRANSFER_HEADER_SIZE = 8 + 4;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final DefaultHAService haService;
    private final SocketChannel socketChannel;
    private final String clientAddress;
    private WriteSocketService writeSocketService;
    private ReadSocketService readSocketService;
    private volatile HAConnectionState currentState = HAConnectionState.TRANSFER;

    // 从服务器请求拉取消息的偏移量
    private volatile long slaveRequestOffset = -1;
    // 从服务器反馈已拉取完成的消息偏移量
    private volatile long slaveAckOffset = -1;
    private FlowMonitor flowMonitor;

    public DefaultHAConnection(final DefaultHAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddress = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        if (NettySystemConfig.socketSndbufSize > 0) {
            this.socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
        }
        if (NettySystemConfig.socketRcvbufSize > 0) {
            this.socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
        }
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
        this.flowMonitor = new FlowMonitor(haService.getDefaultMessageStore().getMessageStoreConfig());
    }

    public void start() {
        changeCurrentState(HAConnectionState.TRANSFER);
        this.flowMonitor.start();
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.flowMonitor.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public String getClientAddress() {
        return this.clientAddress;
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    public long getTransferFromWhere() {
        return writeSocketService.getNextTransferFromWhere();
    }

    class ReadSocketService extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        private final Selector selector;
        private final SocketChannel socketChannel;
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);


        // byteBuffer当前处理指针
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        log.error("processReadEvent error");
                        break;
                    }

                    long interval = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + DefaultHAConnection.this.clientAddress + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            changeCurrentState(HAConnectionState.SHUTDOWN);

            this.makeStop();

            writeSocketService.makeStop();

            haService.removeConnection(DefaultHAConnection.this);

            DefaultHAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                log.error("", e);
            }

            flowMonitor.shutdown(true);

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + ReadSocketService.class.getSimpleName();
            }
            return ReadSocketService.class.getSimpleName();
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            /**
             * 第一步：如果byteBufferRead没有剩余空间，说明该position==limit==capacity，调用byteBufferRead.flip()方法，
             * 产生的效果为position=0、limit=capacity并设置processPostion为0，表示从头开始处理，
             * 其实这里调用byteBuffer.clear()方法会更加容易理解，
             */
            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 处理网络读
                    int readSize = this.socketChannel.read(this.byteBufferRead);

                    /**
                     * 如果读取的字节大于0并且本次读取到的内容大于等于8，表明收到了从服务器一条拉取消息的请求。
                     * 由于有新的从服务器反馈拉取偏移量，服务端会通知由于同步等待主从复制结果而阻塞的消息发送者线程，
                     */
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        if ((this.byteBufferRead.position() - this.processPosition) >= DefaultHAClient.REPORT_HEADER_SIZE) {
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % DefaultHAClient.REPORT_HEADER_SIZE);
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPosition = pos;

                            // 从服务器反馈已拉取完成的消息偏移量
                            DefaultHAConnection.this.slaveAckOffset = readOffset;
                            if (DefaultHAConnection.this.slaveRequestOffset < 0) {
                                DefaultHAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + DefaultHAConnection.this.clientAddress + "] request offset " + readOffset);
                            }

                            // 通知由于同步等待主从复制结果而阻塞的消息发送者线程
                            DefaultHAConnection.this.haService.notifyTransferSome(DefaultHAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        // 如果读取到的字节数等于0，则重复执行三次读请求，否则结束本次读请求处理
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + DefaultHAConnection.this.clientAddress + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;

        // 消息头长度，即消息物理偏移量+消息长度
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);

        // 下一次传输的物理偏移量
        private long nextTransferFromWhere = -1;
        // 根据偏移量查找消息的结果
        private SelectMappedBufferResult selectMappedBufferResult;
        // 上一次数据是否传输完毕
        private boolean lastWriteOver = true;
        private long lastPrintTimestamp = System.currentTimeMillis();
        // 上次写入消息的时间戳
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    // 如果slaveRequestOffset等于-1，说明主服务器还未收到从服务器的拉取请求，则放弃本次事件处理
                    if (-1 == DefaultHAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    // 如果nextTransferFromWhere为-1，表示初次进行数据传输，计算待传输的物理偏移量，
                    // 如果slaveRequestOffset为0，则从当前CommitLog文件最大偏移量开始传输，否则根据从服务器的拉取请求偏移量开始传输
                    if (-1 == this.nextTransferFromWhere) {
                        if (0 == DefaultHAConnection.this.slaveRequestOffset) {
                            long masterOffset = DefaultHAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            masterOffset =
                                masterOffset
                                    - (masterOffset % DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            this.nextTransferFromWhere = DefaultHAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + DefaultHAConnection.this.clientAddress
                            + "], and slave request " + DefaultHAConnection.this.slaveRequestOffset);
                    }

                    /**
                     * 如果已全部写入，且当前系统时间与上次最后写入的时间间隔大于高可用心跳检测时间，则发送一个心跳包，
                     * 心跳包的长度为12个字节（从服务器待拉取偏移量+size），消息长度默认为0，避免长连接由于空闲被关闭。
                     * 高可用心跳包发送间隔通过haSendHeartbeatInterval设置，默认值为5s
                     */
                    if (this.lastWriteOver) {

                        long interval =
                            DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        // 如果上次数据未写完，则先传输上一次的数据，如果消息还是未全部传输，则结束此次事件处理
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 传输消息到从服务器

                    /**
                     * 根据消息从服务器请求的待拉取消息偏移量，查找该偏移量之后所有的可读消息，如果未查到匹配的消息，
                     * 通知所有等待线程继续等待100ms。
                     */
                    SelectMappedBufferResult selectResult =
                        DefaultHAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);

                    /**
                     * 如果匹配到消息，且查找到的消息总长度大于配置高可用传输一次同步任务的最大传输字节数，则通过设置ByteBuffer的limit来控制只传输指定长度的字节，
                     * 这就意味着高可用客户端收到的消息会包含不完整的消息。高可用一批次传输消息最大字节通过haTransferBatchSize设置，默认值为32KB。
                     */
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        if (size > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();
                        if (size > canTransferMaxBytes) {
                            if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
                                log.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
                                    String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0),
                                    String.format("%.2f", flowMonitor.getTransferredByteInSecond() / 1024.0));
                                lastPrintTimestamp = System.currentTimeMillis();
                            }
                            size = canTransferMaxBytes;
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    } else {

                        // 通知所有等待线程继续等待100ms。
                        DefaultHAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    DefaultHAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            DefaultHAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            changeCurrentState(HAConnectionState.SHUTDOWN);

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(DefaultHAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                DefaultHAConnection.log.error("", e);
            }

            flowMonitor.shutdown(true);

            DefaultHAConnection.log.info(this.getServiceName() + " service end");
        }

        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    flowMonitor.addByteCountTransferred(writeSize);
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + WriteSocketService.class.getSimpleName();
            }
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        public long getNextTransferFromWhere() {
            return nextTransferFromWhere;
        }
    }
}
