/*
 * Copyright 2018 Sam Pullara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package nbdfdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.subspace.Subspace;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class FDBBitSet {
  private final Database database;
  private final Subspace subspace;
  private final int blockSize;
  private final byte[] allSetBytes;
  private final Range subspaceRange;

  protected FDBBitSet(Database database, Subspace subspace, int blockSize) {
    this.database = database;
    this.subspace = subspace;
    this.blockSize = blockSize;
    RoaringBitmap allSet = new RoaringBitmap();
    allSet.add(0L, blockSize * 8L);
    ByteBuffer byteBuffer = ByteBuffer.allocate(allSet.serializedSizeInBytes());
    allSet.serialize(byteBuffer);
    allSetBytes = byteBuffer.array();
    subspaceRange = Range.startsWith(subspace.pack());
  }

  public CompletableFuture<Void> set(long startBit, long endBit) {
    return database.runAsync(tx -> {
      FDBBitSet.this.set(tx, startBit, endBit);
      return CompletableFuture.completedFuture(null);
    });
  }

  protected void set(Transaction tx, long startBit, long endBit) {
    MutableRoaringBitmap bitSet = new MutableRoaringBitmap();
    bitSet.add(startBit, endBit);
    ByteBuffer byteBuffer = ByteBuffer.allocate(bitSet.serializedSizeInBytes());
    bitSet.serialize(byteBuffer);
    byte[] bytes = byteBuffer.array();
    tx.set(subspace.pack(), bytes);
  }

  public CompletableFuture<Long> count() {
    return database.runAsync(tx -> {
      long count = 0;
      for (KeyValue keyValue : tx.getRange(subspaceRange)) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(keyValue.getValue());
        ImmutableRoaringBitmap bitSet = new ImmutableRoaringBitmap(byteBuffer);
        count += bitSet.getLongCardinality();
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
