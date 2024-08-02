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
package org.apache.rocketmq.store;

import java.util.Map;

public class DispatchRequest {
    private final String topic;
    private final int queueId;
    // 消息物理偏移量
    private final long commitLogOffset;
    // 消息长度
    private int msgSize;
    // 消息过滤tag哈希码
    private final long tagsCode;
    // 消息存储时间戳
    private final long storeTimestamp;
    // 消息队列偏移量
    private final long consumeQueueOffset;
    // 消息索引key。多个索引key用空格隔开，例如key1 key2。
    private final String keys;
    // 是否成功解析到完整的消息
    private final boolean success;
    // 消息唯一键
    private final String uniqKey;

    // 消息系统标记
    private final int sysFlag;
    // 消息预处理事务偏移量
    private final long preparedTransactionOffset;
    // 消息属性
    private final Map<String, String> propertiesMap;
    // 位图
    private byte[] bitMap;

    private int bufferSize = -1;//the buffer size maybe larger than the msg size if the message is wrapped by something

    // for batch consume queue
    private long  msgBaseOffset = -1;
    private short batchSize = 1;

    private long nextReputFromOffset = -1;

    private String offsetId;

    public DispatchRequest(
        final String topic,
        final int queueId,
        final long commitLogOffset,
        final int msgSize,
        final long tagsCode,
        final long storeTimestamp,
        final long consumeQueueOffset,
        final String keys,
        final String uniqKey,
        final int sysFlag,
        final long preparedTransactionOffset,
        final Map<String, String> propertiesMap
    ) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.msgBaseOffset = consumeQueueOffset;
        this.keys = keys;
        this.uniqKey = uniqKey;

        this.sysFlag = sysFlag;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.success = true;
        this.propertiesMap = propertiesMap;
    }

    public DispatchRequest(String topic, int queueId, long consumeQueueOffset, long commitLogOffset, int size, long tagsCode) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = size;
        this.tagsCode = tagsCode;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int size) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int size, boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = success;
        this.propertiesMap = null;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }

    public String getKeys() {
        return keys;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public byte[] getBitMap() {
        return bitMap;
    }

    public void setBitMap(byte[] bitMap) {
        this.bitMap = bitMap;
    }

    public short getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(short batchSize) {
        this.batchSize = batchSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public long getMsgBaseOffset() {
        return msgBaseOffset;
    }

    public void setMsgBaseOffset(long msgBaseOffset) {
        this.msgBaseOffset = msgBaseOffset;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public long getNextReputFromOffset() {
        return nextReputFromOffset;
    }

    public void setNextReputFromOffset(long nextReputFromOffset) {
        this.nextReputFromOffset = nextReputFromOffset;
    }

    public String getOffsetId() {
        return offsetId;
    }

    public void setOffsetId(String offsetId) {
        this.offsetId = offsetId;
    }

    @Override
    public String toString() {
        return "DispatchRequest{" +
                "topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", commitLogOffset=" + commitLogOffset +
                ", msgSize=" + msgSize +
                ", success=" + success +
                ", msgBaseOffset=" + msgBaseOffset +
                ", batchSize=" + batchSize +
                ", nextReputFromOffset=" + nextReputFromOffset +
            '}';
    }
}
