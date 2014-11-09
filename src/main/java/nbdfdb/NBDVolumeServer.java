package nbdfdb;

import com.foundationdb.async.Future;
import com.foundationdb.async.PartialFunction;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import com.sampullara.fdb.FDBArray;
import nbdfdb.NBD.Command;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.LongAdder;

import static nbdfdb.NBD.EMPTY_124;
import static nbdfdb.NBD.NBD_FLAG_HAS_FLAGS;
import static nbdfdb.NBD.NBD_FLAG_SEND_FLUSH;
import static nbdfdb.NBD.NBD_OK_BYTES;
import static nbdfdb.NBD.NBD_REPLY_MAGIC_BYTES;
import static nbdfdb.NBD.NBD_REQUEST_MAGIC;
import static nbdfdb.NBD.REP_MAGIC_BYTES;

/**
* Created by sam on 11/9/14.
*/
class NBDVolumeServer implements Runnable {
  private final Socket accept;
  private final FDBArray fdbArray;
  private final long size;
  private final DataInputStream in;
  private final DataOutputStream out;
  private final LongAdder writesStarted;
  private final LongAdder writesComplete;

  public NBDVolumeServer(Socket accept, FDBArray fdbArray, long size, DataInputStream in, DataOutputStream out) throws IOException {
    this.accept = accept;
    this.fdbArray = fdbArray;
    this.size = size;
    this.in = in;
    this.out = out;
    writesStarted = new LongAdder();
    writesComplete = new LongAdder();
  }

  private void writeReplyHeaderAndFlush(long handle) throws IOException {
    synchronized (out) {
      out.write(NBD_REPLY_MAGIC_BYTES);
      out.write(NBD_OK_BYTES);
      out.writeLong(handle);
      out.flush();
    }
  }

  @Override
  public void run() {
    try {
      out.writeLong(size);
      out.writeShort(NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH);
      out.write(EMPTY_124);
      out.flush();

      while (true) {
        int requestMagic = in.readInt();// MAGIC
        if (requestMagic != NBD_REQUEST_MAGIC) {
          throw new IllegalArgumentException("Invalid magic number for request: " + requestMagic);
        }
        Command requestType = Command.values()[in.readInt()];
        long handle = in.readLong();
        UnsignedLong offset = UnsignedLong.fromLongBits(in.readLong());
        UnsignedInteger requestLength = UnsignedInteger.fromIntBits(in.readInt());
        if (requestLength.longValue() > Integer.MAX_VALUE) {
          // We could ultimately support this but it isn't common by any means
          throw new IllegalArgumentException("Failed to read, length too long: " + requestLength);
        }
        switch (requestType) {
          case READ: {
            byte[] buffer = new byte[requestLength.intValue()];
            Future<Void> read = fdbArray.read(buffer, offset.intValue());
            read.map(new PartialFunction<Void, Object>() {
              @Override
              public Object apply(Void aVoid) throws Exception {
                synchronized (out) {
                  out.write(REP_MAGIC_BYTES);
                  out.write(NBD_OK_BYTES);
                  out.writeLong(handle);
                  out.write(buffer);
                  out.flush();
                }
                return null;
              }
            });
            break;
          }
          case WRITE: {
            byte[] buffer = new byte[requestLength.intValue()];
            in.readFully(buffer);
            writesStarted.increment();
            Future<Void> write = fdbArray.write(buffer, offset.intValue());
            write.map((PartialFunction<Void, Object>) aVoid -> {
              writeReplyHeaderAndFlush(handle);
              writesComplete.increment();
              synchronized (writesComplete) {
                writesComplete.notifyAll();
              }
              return null;
            });
            break;
          }
          case DISCOVER:
            accept.close();
            break;
          case FLUSH:
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
            writeReplyHeaderAndFlush(handle);
            break;
          case TRIM:
            System.out.println("Trim");
            writeReplyHeaderAndFlush(handle);
            break;
          case CACHE:
            System.out.println("Cache");
            break;
          default:
            System.out.println("What command? " + requestType);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
