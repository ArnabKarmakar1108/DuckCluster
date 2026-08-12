package io.duckcluster.common.shard;

import java.util.zip.CRC32;

public final class ShardTransferChecksum {
    private ShardTransferChecksum() {}

    public static long crc32(byte[] data, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(data, offset, length);
        return crc.getValue();
    }

    public static void update(CRC32 crc, byte[] data, int offset, int length) {
        crc.update(data, offset, length);
    }

    public static CRC32 newCrc() {
        return new CRC32();
    }
}
