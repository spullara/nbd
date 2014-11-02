package nbdfdb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.async.Future;
import com.foundationdb.async.PartialFunction;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import com.sampullara.fdb.FDBArray;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

import static java.util.Arrays.asList;

public class SocketServer {

  public static void main(String[] args) throws IOException {
    Database db = FDB.selectAPIVersion(200).open();
    DirectorySubspace ds = DirectoryLayer.getDefault().createOrOpen(db, asList("com.sampullara.fdb", "nbdfdb")).get();
    FDBArray fdbArray = new FDBArray(db, ds, 512);
    ExecutorService es = Executors.newCachedThreadPool();
    ServerSocket ss = new ServerSocket(10809);
    while (true) {
      Socket accept = ss.accept();
      es.submit(() -> {
        try {
          DataInputStream in = new DataInputStream(accept.getInputStream());
          DataOutputStream os = new DataOutputStream(new BufferedOutputStream(accept.getOutputStream()));

          os.write("NBDMAGIC".getBytes());
          os.write(new byte[]{0x49, 0x48, 0x41, 0x56, 0x45, 0x4F, 0x50, 0x54});
          os.writeShort(1);
          os.flush();

          int clientFlags = in.readInt();
          long magic = in.readLong();
          int opt = in.readInt();
          if (opt != 1) {
            throw new RuntimeException("We support only EXPORT options");
          }
          int length = in.readInt();
          byte[] bytes = new byte[length];
          in.readFully(bytes);
          os.writeLong(1_000_000_000); // 1 GiB
          os.writeShort(1 + 4); // FLUSH
          os.write(new byte[124]);
          os.flush();

          LongAdder writesStarted = new LongAdder();
          LongAdder writesComplete = new LongAdder();
          while (true) {
            in.readInt(); // MAGIC
            int requestType = in.readInt();
            long handle = in.readLong();
            UnsignedLong offset = UnsignedLong.fromLongBits(in.readLong());
            UnsignedInteger requestLength = UnsignedInteger.fromIntBits(in.readInt());
            if (requestLength.longValue() > Integer.MAX_VALUE) {
              throw new RuntimeException("Failed to read, length too long: " + requestLength);
            }
            switch (requestType) {
              case 0: { // NBD_CMD_READ
                byte[] buffer = new byte[requestLength.intValue()];
                Future<Void> read = fdbArray.read(buffer, offset.intValue());
                read.map(new PartialFunction<Void, Object>() {
                  @Override
                  public Object apply(Void aVoid) throws Exception {
                    synchronized (os) {
                      os.writeInt(0x67446698); // MAGIC
                      os.writeInt(0); // OK
                      os.writeLong(handle);
                      os.write(buffer);
                      os.flush();
                    }
                    return null;
                  }
                });
                break;
              }
              case 1: { // NBD_CMD_WRITE
                byte[] buffer = new byte[requestLength.intValue()];
                in.readFully(buffer);
                writesStarted.increment();
                Future<Void> write = fdbArray.write(offset.intValue(), buffer);
                write.map(new PartialFunction<Void, Object>() {
                  @Override
                  public Object apply(Void aVoid) throws Exception {
                    writeReplyHeader(os, handle);
                    writesComplete.increment();
                    synchronized (writesComplete) {
                      writesComplete.notifyAll();
                    }
                    return null;
                  }
                });
                break;
              }
              case 2: // NBD_CMD_DISC
                accept.close();
                break;
              case 3: // NBD_CMD_FLUSH
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
                writeReplyHeader(os, handle);
                break;
              case 4: // NBD_CMD_TRIM
                System.out.println("Trim");
                writeReplyHeader(os, handle);
                break;
              case 5: // NBD_CMD_CACHE
                System.out.println("Cache");
                break;
              default:
                System.out.println("What command? " + requestType);
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
  }

  private static void writeReplyHeader(DataOutputStream os, long handle) throws IOException {
    synchronized (os) {
      os.writeInt(0x67446698);
      os.writeInt(0); // OK
      os.writeLong(handle);
      os.flush();
    }
  }
}
