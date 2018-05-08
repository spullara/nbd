package nbdfdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.subspace.Subspace;

import java.util.BitSet;
import java.util.concurrent.CompletableFuture;

public class FDBBitSet {
  private final Database database;
  private final Subspace subspace;
  private final int blockSize;
  private final int bitsPerBlock;
  private final byte[] allSetBytes;
  private final Range subspaceRange;

  protected FDBBitSet(Database database, Subspace subspace, int blockSize) {
    this.database = database;
    this.subspace = subspace;
    this.blockSize = blockSize;
    bitsPerBlock = blockSize * 8;
    BitSet allSet = new BitSet(bitsPerBlock);
    allSet.set(0, bitsPerBlock);
    allSetBytes = allSet.toByteArray();
    subspaceRange = Range.startsWith(subspace.pack());
  }

  public CompletableFuture<Void> set(long startBit, long endBit) {
    return database.runAsync(tx -> {
      FDBBitSet.this.set(tx, startBit, endBit);
      return CompletableFuture.completedFuture(null);
    });
  }

  protected void set(Transaction tx, long startBit, long endBit) {
    long startBlock = startBit / bitsPerBlock;
    long endBlock = endBit / bitsPerBlock;
    BitSet bitSet = new BitSet(bitsPerBlock);
    for (long block = startBlock; block <= endBlock; block++) {
      bitSet.clear();
      if (block == startBlock) {
        int startBitOffset = (int) (startBit % bitsPerBlock);
        int endBitOffset = startBlock == endBlock ? (int) (endBit % bitsPerBlock) : bitsPerBlock - 1;
        bitSet.set(startBitOffset, endBitOffset + 1);
        byte[] bitBytes = bitSet.toByteArray();
        byte[] bytes = new byte[blockSize];
        System.arraycopy(bitBytes, 0, bytes, 0, bitBytes.length);
        tx.mutate(MutationType.BIT_OR, subspace.get(block).pack(), bytes);
      } else if (block == endBlock) {
        int endBitOffset = (int) (endBit % bitsPerBlock);
        bitSet.set(0, endBitOffset);
        byte[] bitBytes = bitSet.toByteArray();
        byte[] bytes = new byte[blockSize];
        System.arraycopy(bitBytes, 0, bytes, 0, bitBytes.length);
        tx.mutate(MutationType.BIT_OR, subspace.get(block).pack(), bytes);
      } else {
        tx.set(subspace.get(block).pack(), allSetBytes);
      }
    }
  }

  public CompletableFuture<Long> count() {
    return database.runAsync(tx -> {
      long count = 0;
      for (KeyValue keyValue : tx.getRange(subspaceRange)) {
        count += BitSet.valueOf(keyValue.getValue()).cardinality();
      }
      return CompletableFuture.completedFuture(count);
    });
  }

  public void clear() {
    database.run(tx -> {
      tx.clear(subspaceRange);
      return null;
    });
  }

  public void clear(Transaction tx) {
    tx.clear(subspaceRange);
  }
}
