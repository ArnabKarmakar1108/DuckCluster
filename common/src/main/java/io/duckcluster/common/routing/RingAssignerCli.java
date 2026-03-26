package io.duckcluster.common.routing;

import java.util.List;

/**
 * CLI tool that prints shard-to-worker assignments as JSON.
 * Used by split-and-distribute.sh to determine shard placement.
 *
 * Usage: java -cp <classpath> io.duckcluster.common.routing.RingAssignerCli
 *            --workers worker-1,worker-2,worker-3
 *            --table events
 *            --shards 6
 *            --rf 2
 *            [--vnodes 100]
 */
public final class RingAssignerCli {

    public static void main(String[] args) {
        String workers = null;
        String table = null;
        int shards = -1;
        int rf = -1;
        int vnodes = 100;

        for (int i = 0; i < args.length - 1; i += 2) {
            switch (args[i]) {
                case "--workers" -> workers = args[i + 1];
                case "--table" -> table = args[i + 1];
                case "--shards" -> shards = Integer.parseInt(args[i + 1]);
                case "--rf" -> rf = Integer.parseInt(args[i + 1]);
                case "--vnodes" -> vnodes = Integer.parseInt(args[i + 1]);
            }
        }

        if (workers == null || table == null || shards < 1 || rf < 1) {
            System.err.println("Usage: RingAssignerCli --workers w1,w2,w3 --table <name> --shards <N> --rf <N> [--vnodes <N>]");
            System.exit(1);
        }

        ConsistentHashRing ring = new ConsistentHashRing(vnodes);
        String[] workerIds = workers.split(",");
        for (String w : workerIds) {
            ring.addWorker(w.trim());
        }

        StringBuilder json = new StringBuilder("{\n");
        for (int i = 0; i < shards; i++) {
            String key = table + ":" + i;
            List<String> owners = ring.getOwners(key, rf);
            json.append("  \"").append(table).append("_shard").append(i).append("\": [");
            for (int j = 0; j < owners.size(); j++) {
                if (j > 0) json.append(", ");
                json.append("\"").append(owners.get(j)).append("\"");
            }
            json.append("]");
            if (i < shards - 1) json.append(",");
            json.append("\n");
        }
        json.append("}");
        System.out.println(json);
    }
}
