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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.google.common.primitives.Longs;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.*;

public class FDBStorage implements Storage {
  private static final long _30_SECONDS = MILLISECONDS.convert(30, SECONDS);
  private static final long _1_MINUTE = MILLISECONDS.convert(1, MINUTES);
  private static final byte[] ZERO = Longs.toByteArray(0);

  private static final Database db = FDB.selectAPIVersion(510).open();
  private static final ExecutorService es = Executors.newFixedThreadPool(1, r -> new Thread(r, "fdbstorage-flush"));
  private static final Timer timer = new Timer("connection-leases");

  private final FDBArray fdbArray;
  private final LongAdder writesStarted;
  private final LongAdder writesComplete;
  private final long size;
  private final String exportName;

  private TimerTask leaseTask;

  public FDBStorage(String exportName) {
    this.exportName = exportName;
    writesStarted = new LongAdder();
    writesComplete = new LongAdder();
    fdbArray = FDBArray.open(db, exportName);
    byte[] sizeBytes = fdbArray.getMetadata(NBD.SIZE_KEY);
    if (sizeBytes == null) {
      throw new IllegalArgumentException("Size of volume not configured");
    }
    size = Longs.fromByteArray(sizeBytes);
  }

  @Override
  public synchronized void connect() {
    byte[] lease = fdbArray.getMetadata(NBD.LEASE_KEY);
    if (lease == null || (System.currentTimeMillis() - Longs.fromByteArray(lease) > _1_MINUTE)) {
      if (leaseTask != null) leaseTask.cancel();
      leaseTask = new TimerTask() {
        @Override
        public void run() {
          fdbArray.setMetadata(NBD.LEASE_KEY, Longs.toByteArray(System.currentTimeMillis()));
        }
      };
      timer.schedule(leaseTask, 0, _30_SECONDS);
    } else {
      throw new IllegalStateException("Volume " + exportName + " is already leased");
    }
  }

  @Override
  public synchronized void disconnect() {
    if (leaseTask != null) {
      leaseTask.cancel();
      fdbArray.setMetadata(NBD.LEASE_KEY, ZERO);
    } else {
      throw new IllegalStateException("Not connected to " + exportName);
    }
  }

  @Override
  public CompletableFuture<Void> read(byte[] buffer, long offset) {
    return fdbArray.read(buffer, offset);
  }

  @Override
  public CompletableFuture<Void> write(byte[] buffer, long offset) {
    writesStarted.increment();
    CompletableFuture<Void> write = fdbArray.write(buffer, offset);
    write.thenRun(() -> {
      writesComplete.increment();
      synchronized (writesComplete) {
        writesComplete.notifyAll();
      }
    });
    return write;
  }

  @Override
  public CompletableFuture<Void> flush() {
    CompletableFuture<Void> result = new CompletableFuture<>();
    es.submit(() -> {
      synchronized (writesComplete) {
        long target = writesStarted.longValue();
        while (target > writesComplete.longValue()) {
          try {
            writesComplete.wait();
          } catch (InterruptedException e) {
            // Ignore and continue looping
          }
        }
      }
      result.complete(null);
    });
    return result;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public long usage() {
    try {
      return fdbArray.usage().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
