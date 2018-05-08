package nbdfdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Block storage in FDB
 */
public class FDBArray {

  // FDB
  private static final byte[] ONE = new byte[]{0, 0, 0, 0, 0, 0, 0, 1};
  private static final byte[] MINUS_ONE = new byte[]{0, 0, 0, 0, 0, 0, 0, -1};
  private static DirectoryLayer dl = DirectoryLayer.getDefault();

  // Metadata keys
  private static final String BLOCK_SIZE_KEY = "block_size";
  private static final String PARENT_KEY = "parent";
  private static final String PARENT_TIMESTAMP_KEY = "parent_timestamp";
  private static final String DEPENDENTS = "dependents";
  private static final String BLOCKS = "blocks";

  // Location in the database
  private final DirectorySubspace metadata;
  private final DirectorySubspace data;
  private final Database database;
  private final int blockSize;
  private final FDBArray parentArray;
  private final DirectorySubspace ds;
  private final Long snapshot;
  private final FDBBitSet usedBlocks;

  // Keys
  private byte[] dependents;

  // Used for copies
  private final ThreadLocal<byte[]> buffer = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[blockSize];
    }
  };

  public static FDBArray open(Database database, String name) {
    DirectorySubspace ds = get(dl.open(database, asList("com.sampullara.fdb.array", name)));
    return new FDBArray(database, ds);
  }

  public static FDBArray open(Database database, String name, long timestamp) {
    DirectorySubspace ds = get(dl.open(database, asList("com.sampullara.fdb.array", name)));
    return new FDBArray(database, ds, timestamp);
  }

  public static FDBArray create(Database database, String name, int blockSize) {
    DirectorySubspace ds = get(dl.create(database, asList("com.sampullara.fdb.array", name)));
    return create(database, ds, blockSize, null, 0);
  }

  protected static FDBArray create(Database database, DirectorySubspace ds, int blockSize, DirectorySubspace parent, long timestamp) {
    DirectorySubspace metadata = get(ds.create(database, singletonList("metadata")));
    if (parent != null) {
      List<String> parentPath = parent.getPath();
      database.run((Function<Transaction, Void>) tx -> {
        tx.set(metadata.get(PARENT_KEY).pack(), Tuple.fromList(parentPath).pack());
        tx.set(metadata.get(PARENT_TIMESTAMP_KEY).pack(), Tuple.from(timestamp).pack());
        return null;
      });
    }
    database.run((Function<Transaction, Void>) tx -> {
      tx.set(metadata.get(BLOCK_SIZE_KEY).pack(), Ints.toByteArray(blockSize));
      return null;
    });
    return new FDBArray(database, ds);
  }

  private static <T> T get(CompletableFuture<T> future) {
    try {
      return future.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected FDBArray(Database database, DirectorySubspace ds, Long snapshot) {
    this.ds = ds;
    this.snapshot = snapshot;
    this.database = database;
    this.metadata = get(ds.createOrOpen(database, singletonList("metadata")));
    this.data = get(ds.createOrOpen(database, singletonList("data")));
    Integer currentBlocksize = database.run(tx -> {
      byte[] key = metadata.get(BLOCK_SIZE_KEY).pack();
      byte[] currentBlockSize = get(tx.get(key));
      if (currentBlockSize == null) {
        return null;
      } else {
        return Ints.fromByteArray(currentBlockSize);
      }
    });
    if (currentBlocksize == null) {
      throw new IllegalArgumentException("Block size for array not configured");
    } else {
      blockSize = currentBlocksize;
    }
    parentArray = database.run(tx -> {
      byte[] parentPathValue = get(tx.get(metadata.get(PARENT_KEY).pack()));
      byte[] parentTimestampBytes = get(tx.get(metadata.get(PARENT_TIMESTAMP_KEY).pack()));
      if (parentPathValue == null) {
        return null;
      } else {
        List<String> items = (List) Tuple.fromBytes(parentPathValue).getItems();
        long parentTimestamp = Tuple.fromBytes(parentTimestampBytes).getLong(0);
        return new FDBArray(database, get(DirectoryLayer.getDefault().open(database, items)), parentTimestamp);
      }
    });
    dependents = metadata.get(DEPENDENTS).pack();
    usedBlocks = new FDBBitSet(database, metadata.get(BLOCKS), 512);
  }

  protected FDBArray(Database database, DirectorySubspace ds) {
    this(database, ds, null);
  }

  public CompletableFuture<Void> write(byte[] write, long offset) {
    if (snapshot != null) {
      throw new IllegalStateException("FDBArray is read only");
    }
    return database.runAsync(tx -> {
      // Use a single buffer for all full blocksize writes
      byte[] bytes = buffer.get();

      // Calculate the block locations
      int length = write.length;
      long firstBlock = offset / blockSize;
      long lastBlock = (offset + length) / blockSize;
      int blockOffset = (int) (offset % blockSize);
      int shift = blockSize - blockOffset;

      // Track where we have written so we can estimate usage later
      usedBlocks.set(firstBlock, lastBlock);

      // Special case first block and last block
      byte[] firstBlockKey = data.get(firstBlock).get(System.currentTimeMillis()).pack();
      if (blockOffset > 0 || (blockOffset == 0 && length < blockSize)) {
        // Only need to do this if the first block is partial
        byte[] readBytes = new byte[blockSize];
        read(tx, firstBlock * blockSize, readBytes, Long.MAX_VALUE, null);
        int writeLength = Math.min(length, shift);
        System.arraycopy(write, 0, readBytes, blockOffset, writeLength);
        tx.set(firstBlockKey, readBytes);
      } else {
        // In this case copy the full first block blindly
        System.arraycopy(write, 0, bytes, 0, blockSize);
        tx.set(firstBlockKey, bytes);
      }
      // If there is more than one block
      if (lastBlock > firstBlock) {
        // For the blocks in the middle we can just blast values in without looking at the current bytes
        for (long i = firstBlock + 1; i < lastBlock; i++) {
          byte[] key = data.get(i).get(System.currentTimeMillis()).pack();
          int writeBlock = (int) (i - firstBlock);
          int position = (writeBlock - 1) * blockSize + shift;
          System.arraycopy(write, position, bytes, 0, blockSize);
          tx.set(key, bytes);
        }
        int position = (int) ((lastBlock - firstBlock - 1) * blockSize + shift);
        int lastBlockLength = length - position;
        byte[] lastBlockKey = data.get(lastBlock).get(System.currentTimeMillis()).pack();
        // If the last block is a complete block we don't need to read
        if (lastBlockLength == blockSize) {
          System.arraycopy(write, position, bytes, 0, blockSize);
          tx.set(lastBlockKey, bytes);
        } else {
          byte[] readBytes = new byte[blockSize];
          read(tx, lastBlock * blockSize, readBytes, Long.MAX_VALUE, null);
          System.arraycopy(write, position, readBytes, 0, lastBlockLength);
          tx.set(lastBlockKey, readBytes);
        }
      }
      return CompletableFuture.completedFuture(null);
    });
  }

  public CompletableFuture<Long> usage() {
    return usedBlocks.count().thenApply(usedBlocks -> usedBlocks * blockSize);
  }

  /**
   * Read latest blocks.
   *
   * @param read
   * @param offset
   * @return
   */
  public CompletableFuture<Void> read(byte[] read, long offset) {
    return read(read, offset, Long.MAX_VALUE);
  }

  /**
   * Read blocks as of a particular timestamp.
   *
   * @param read
   * @param offset
   * @param timestamp
   * @return
   */
  public CompletableFuture<Void> read(byte[] read, long offset, long timestamp) {
    return database.runAsync(tx -> {
      read(tx, offset, read, timestamp, null);
      return CompletableFuture.completedFuture(null);
    });
  }

  static class BlocksRead {
    private final int total;
    private Set<Long> blocksRead;

    BlocksRead(int total) {
      this.total = total;
      blocksRead = new HashSet<>(total);
    }

    boolean done() {
      return blocksRead.size() == total;
    }

    boolean read(long block) {
      return blocksRead.add(block);
    }
  }

  private void read(ReadTransaction tx, long offset, byte[] read, long readTimestamp, BlocksRead blocksRead) {
    long snapshotTimestamp = snapshot == null ? readTimestamp : Math.min(readTimestamp, snapshot);
    long firstBlock = offset / blockSize;
    int blockOffset = (int) (offset % blockSize);
    int length = read.length;
    long lastBlock = (offset + length) / blockSize;
    long currentBlockId = -1;
    byte[] currentValue = null;
    if (parentArray != null && blocksRead == null) {
      blocksRead = new BlocksRead((int) (lastBlock - firstBlock + 1));
    }
    for (KeyValue keyValue : tx.getRange(data.get(firstBlock).pack(), data.get(lastBlock + 1).pack())) {
      Tuple keyTuple = data.unpack(keyValue.getKey());
      long blockId = keyTuple.getLong(0);
      if (blockId != currentBlockId && currentBlockId != -1) {
        // Only copy blocks that we are going to use
        copy(read, firstBlock, blockOffset, lastBlock, currentValue, currentBlockId, blocksRead);
        currentValue = null;
      }
      // Advance the current block id
      currentBlockId = blockId;
      // Update the current value with the latest value not written after the snapshot timestamp
      long timestamp = keyTuple.getLong(1);
      if (timestamp <= snapshotTimestamp) {
        currentValue = keyValue.getValue();
      }
    }
    copy(read, firstBlock, blockOffset, lastBlock, currentValue, currentBlockId, blocksRead);
    if (parentArray != null && !blocksRead.done()) {
      // This is currently less efficient than I would like. Basically you should do the other reads
      // and only call the parent when there are gaps. Instead, we are calling all parents for
      // all reads and that just scales poorly as you make a deeper hierarchy.
      parentArray.read(tx, offset, read, snapshotTimestamp, blocksRead);
    }
  }

  private void copy(byte[] read, long firstBlock, int blockOffset, long lastBlock, byte[] currentValue, long blockId, BlocksRead blocksRead) {
    if (currentValue != null) {
      if (blocksRead == null || blocksRead.read(blockId)) {
        int blockPosition = (int) ((blockId - firstBlock) * blockSize);
        int shift = blockSize - blockOffset;
        if (blockId == firstBlock) {
          int firstBlockLength = Math.min(shift, read.length);
          System.arraycopy(currentValue, blockOffset, read, 0, firstBlockLength);
        } else {
          int position = blockPosition - blockSize + shift;
          if (blockId == lastBlock) {
            int lastLength = read.length - position;
            System.arraycopy(currentValue, 0, read, position, lastLength);
          } else {
            System.arraycopy(currentValue, 0, read, position, blockSize);
          }
        }
      }
    }
  }

  public FDBArray snapshot() {
    return snapshot(System.currentTimeMillis());
  }

  public FDBArray snapshot(long timestamp) {
    return new FDBArray(database, ds, timestamp);
  }

  public FDBArray snapshot(String name) {
    database.run(tx -> {
      tx.mutate(MutationType.ADD, dependents, ONE);
      return null;
    });
    List<String> childDirectory = asList("com.sampullara.fdb.array", name);
    DirectorySubspace childDs = get(DirectoryLayer.getDefault().create(database, childDirectory));
    FDBArray.create(database, childDs, blockSize, ds, System.currentTimeMillis());
    return new FDBArray(database, childDs);
  }

  public void clear() {
    database.run((Function<Transaction, Void>) tx -> {
      tx.clear(data.pack());
      usedBlocks.clear(tx);
      return null;
    });
  }

  private void dependentDeleted() {
    database.run(tx -> {
      tx.mutate(MutationType.ADD, dependents, MINUS_ONE);
      return null;
    });
  }

  public void delete() {
    boolean deletable = database.run(tx -> {
      byte[] bytes = get(tx.get(dependents));
      return bytes == null || Longs.fromByteArray(bytes) == 0;
    });
    if (deletable) {
      if (parentArray != null) parentArray.dependentDeleted();
      get(ds.remove(database));
    } else {
      throw new IllegalStateException("Array still has dependents");
    }
  }

  public void setMetadata(byte[] key, byte[] value) {
    database.run(tx -> {
      tx.set(metadata.get(key).pack(), value);
      return null;
    });
  }

  public byte[] getMetadata(byte[] key) {
    byte[] value = database.run(tx -> get(tx.get(metadata.get(key).pack())));
    return value == null ? parentArray == null ? null : parentArray.getMetadata(key) : value;
  }
}
