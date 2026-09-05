package io.duckcluster.coordinator.catalog;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.common.shard.ShardTransferChecksum;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import com.google.protobuf.ByteString;
import io.duckcluster.proto.v1.ShardDataChunk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;

/** Streams staged shard bytes to the hash-ring owner worker (5.2). */
public final class ShardIngestService {
    private final ClusterCatalog catalog;
    private final ShardCatalog shardCatalog;
    private final WorkerRegistry registry;
    private final WorkerNodeClient workerClient;
    private final int chunkBytes;

    public ShardIngestService(
            ClusterCatalog catalog,
            ShardCatalog shardCatalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            int chunkBytes) {
        this.catalog = catalog;
        this.shardCatalog = shardCatalog;
        this.registry = registry;
        this.workerClient = workerClient;
        this.chunkBytes = Math.max(4096, chunkBytes);
    }

    public void stageShard(String tableName, int shardId, long version, byte[] payload) {
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException("shard payload is empty");
        }
        var table = catalog.table(tableName);
        if (shardId < 0 || shardId >= table.shardCount()) {
            throw new IllegalArgumentException("shardId out of range for table " + tableName);
        }
        List<String> owners = shardCatalog.getOwners(tableName, shardId);
        if (owners.isEmpty()) {
            throw new IllegalStateException("no ring owner for " + tableName + "_shard" + shardId);
        }
        String ownerId = owners.get(0);
        WorkerRegistry.WorkerRecord worker = registry.getWorker(ownerId)
                .orElseThrow(() -> new IllegalStateException("owner worker " + ownerId + " is not registered"));

        CRC32 crc = ShardTransferChecksum.newCrc();
        ShardTransferChecksum.update(crc, payload, 0, payload.length);
        Iterator<ShardDataChunk> chunks = chunkIterator(tableName, shardId, version, payload, crc.getValue());
        boolean accepted = workerClient.pushShardTo(worker, chunks);
        if (!accepted) {
            throw new IllegalStateException("worker rejected staged shard " + tableName + "_shard" + shardId);
        }
    }

    private Iterator<ShardDataChunk> chunkIterator(
            String tableName, int shardId, long version, byte[] payload, long crcValue) {
        List<ShardDataChunk> chunks = new ArrayList<>();
        int offset = 0;
        while (offset < payload.length) {
            int end = Math.min(payload.length, offset + chunkBytes);
            int len = end - offset;
            byte[] slice = new byte[len];
            System.arraycopy(payload, offset, slice, 0, len);
            offset = end;
            boolean last = offset >= payload.length;
            ShardDataChunk.Builder builder = ShardDataChunk.newBuilder()
                    .setTableName(tableName)
                    .setShardId(shardId)
                    .setData(ByteString.copyFrom(slice))
                    .setIsLast(last)
                    .setStagingVersion(version);
            if (last) {
                builder.setContentLength(payload.length).setCrc32((int) crcValue);
            }
            chunks.add(builder.build());
        }
        return chunks.iterator();
    }
}
