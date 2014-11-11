package nbdfdb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.async.Future;
import com.foundationdb.async.SettableFuture;
import com.google.common.primitives.Longs;
import com.sampullara.fdb.FDBArray;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FDBStorage implements Storage {
  private static final long _30_SECONDS = MILLISECONDS.convert(30, SECONDS);
  private static final long _1_MINUTE = MILLISECONDS.convert(1, MINUTES);
  private static final byte[] ZERO = Longs.toByteArray(0);

  private static final Database db = FDB.selectAPIVersion(200).open();
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
  public Future<Void> read(byte[] buffer, long offset) {
    return fdbArray.read(buffer, offset);
  }

  @Override
  public Future<Void> write(byte[] buffer, long offset) {
    writesStarted.increment();
    Future<Void> write = fdbArray.write(buffer, offset);
    write.onReady(() -> {
      writesComplete.increment();
      synchronized (writesComplete) {
        writesComplete.notifyAll();
      }
    });
    return write;
  }

  @Override
  public Future<Void> flush() {
    SettableFuture<Void> result = new SettableFuture<>();
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
      result.set(null);
    });
    return result;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public long usage() {
    return fdbArray.usage().get();
  }
}
