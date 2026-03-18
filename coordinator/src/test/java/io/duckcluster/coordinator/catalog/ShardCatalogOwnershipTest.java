package io.duckcluster.coordinator.catalog;

import io.duckcluster.common.routing.ConsistentHashRing;
import io.duckcluster.proto.v1.ShardOwnership;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ShardCatalogOwnershipTest {

    private ShardCatalog catalog;

    @BeforeEach
    void setUp() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        catalog = new ShardCatalog(ring, 2);
        catalog.onWorkerAdded("worker-0");
        catalog.onWorkerAdded("worker-1");
        catalog.onWorkerAdded("worker-2");
    }

    @Test
    void registerOwnership_tracksActualShards() {
        List<ShardOwnership> shards = List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(0).setRowCount(100).build(),
                ShardOwnership.newBuilder().setTableName("events").setShardId(1).setRowCount(200).build()
        );

        catalog.registerOwnership("worker-0", shards);

        List<String> owners = catalog.getActualOwners("events", 0);
        assertEquals(1, owners.size());
        assertTrue(owners.contains("worker-0"));
    }

    @Test
    void registerOwnership_replacesExistingEntries() {
        List<ShardOwnership> initial = List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(0).setRowCount(100).build()
        );
        catalog.registerOwnership("worker-0", initial);

        List<ShardOwnership> updated = List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(1).setRowCount(200).build()
        );
        catalog.registerOwnership("worker-0", updated);

        assertTrue(catalog.getActualOwners("events", 0).isEmpty());
        assertEquals(List.of("worker-0"), catalog.getActualOwners("events", 1));
    }

    @Test
    void getUnderReplicatedShards_detectsMissingReplicas() {
        List<ShardOwnership> shards = List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(0).setRowCount(100).build()
        );
        catalog.registerOwnership("worker-0", shards);

        List<ShardCatalog.UnderReplicatedShard> underReplicated = catalog.getUnderReplicatedShards();
        assertFalse(underReplicated.isEmpty());

        ShardCatalog.UnderReplicatedShard shard = underReplicated.get(0);
        assertEquals("events", shard.tableName());
        assertEquals(0, shard.shardId());
        assertEquals("worker-0", shard.sourceWorker());
        assertFalse(shard.targetWorkers().isEmpty());
    }

    @Test
    void getUnderReplicatedShards_emptyWhenFullyReplicated() {
        List<String> expectedOwners = catalog.getOwners("events", 0);

        for (String owner : expectedOwners) {
            catalog.registerOwnership(owner, List.of(
                    ShardOwnership.newBuilder().setTableName("events").setShardId(0).setRowCount(100).build()
            ));
        }

        List<ShardCatalog.UnderReplicatedShard> underReplicated = catalog.getUnderReplicatedShards();
        boolean shard0UnderReplicated = underReplicated.stream()
                .anyMatch(s -> s.tableName().equals("events") && s.shardId() == 0);
        assertFalse(shard0UnderReplicated);
    }

    @Test
    void onWorkerRemoved_clearsActualOwnership() {
        catalog.registerOwnership("worker-0", List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(0).setRowCount(100).build()
        ));

        catalog.onWorkerRemoved("worker-0");

        assertTrue(catalog.getActualOwners("events", 0).isEmpty());
    }
}
