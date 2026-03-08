package io.duckcluster.common.routing;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest {

    @Test
    void addWorker_distributesEvenly() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        for (int i = 0; i < 5; i++) {
            ring.addWorker("worker-" + i);
        }

        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            List<String> owners = ring.getOwners("key-" + i, 1);
            counts.merge(owners.get(0), 1, Integer::sum);
        }

        for (int count : counts.values()) {
            assertTrue(count > 100, "Each worker should get >10% of keys, got " + count);
            assertTrue(count < 350, "No worker should get >35% of keys, got " + count);
        }
    }

    @Test
    void removeWorker_minimalMigration() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        for (int i = 0; i < 5; i++) {
            ring.addWorker("worker-" + i);
        }

        Map<String, String> before = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            before.put(key, ring.getOwners(key, 1).get(0));
        }

        ring.removeWorker("worker-2");

        int migrated = 0;
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            String newOwner = ring.getOwners(key, 1).get(0);
            if (!newOwner.equals(before.get(key))) {
                migrated++;
            }
        }

        assertTrue(migrated < 400, "Removing 1/5 workers should migrate ~20% of keys, got " + migrated);
        assertTrue(migrated > 50, "Some keys should migrate, got " + migrated);
    }

    @Test
    void addWorker_minimalMigration() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        for (int i = 0; i < 5; i++) {
            ring.addWorker("worker-" + i);
        }

        Map<String, String> before = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            before.put(key, ring.getOwners(key, 1).get(0));
        }

        ring.addWorker("worker-5");

        int migrated = 0;
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            String newOwner = ring.getOwners(key, 1).get(0);
            if (!newOwner.equals(before.get(key))) {
                migrated++;
            }
        }

        assertTrue(migrated < 350, "Adding 1 worker to 5 should migrate ~17% of keys, got " + migrated);
        assertTrue(migrated > 50, "Some keys should migrate, got " + migrated);
    }

    @Test
    void getOwners_returnsDistinctWorkers() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        for (int i = 0; i < 5; i++) {
            ring.addWorker("worker-" + i);
        }

        for (int i = 0; i < 100; i++) {
            List<String> owners = ring.getOwners("shard-" + i, 3);
            assertEquals(3, owners.size());
            assertEquals(3, new HashSet<>(owners).size(), "Owners must be distinct");
        }
    }

    @Test
    void getOwners_cappedAtWorkerCount() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        ring.addWorker("worker-0");
        ring.addWorker("worker-1");

        List<String> owners = ring.getOwners("key", 5);
        assertEquals(2, owners.size());
    }

    @Test
    void deterministic_sameInputsSameOutput() {
        ConsistentHashRing ring1 = new ConsistentHashRing(100);
        ConsistentHashRing ring2 = new ConsistentHashRing(100);

        for (int i = 0; i < 3; i++) {
            ring1.addWorker("worker-" + i);
            ring2.addWorker("worker-" + i);
        }

        for (int i = 0; i < 100; i++) {
            String key = "events:" + i;
            assertEquals(ring1.getOwners(key, 2), ring2.getOwners(key, 2));
        }
    }

    @Test
    void emptyRing_returnsEmptyList() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        assertTrue(ring.getOwners("key", 2).isEmpty());
    }

    @Test
    void addWorker_idempotent() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        ring.addWorker("worker-0");
        ring.addWorker("worker-0");
        assertEquals(1, ring.getWorkerCount());
    }
}
