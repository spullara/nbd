package nbdfdb;

import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SocketServer {

  public static void main(String[] args) throws IOException {
    ExecutorService es = Executors.newCachedThreadPool();
    ServerSocket ss = new ServerSocket(10809);
    while (true) {
      Socket accept = ss.accept();
      es.submit(() -> {
        try {
          byte[] storage = new byte[64_000_000];
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
          os.writeLong(storage.length);
          os.writeShort(1 + 4 + 32); // FLUSH, TRIM
          os.write(new byte[124]);
          os.flush();

          byte[] handle = new byte[8];
          while (true) {
            in.readInt(); // MAGIC
            int requestType = in.readInt();
            in.readFully(handle);
            UnsignedLong offset = UnsignedLong.fromLongBits(in.readLong());
            UnsignedInteger requestLength = UnsignedInteger.fromIntBits(in.readInt());
            if (requestLength.longValue() > Integer.MAX_VALUE) {
              throw new RuntimeException("Failed to read, length too long: " + requestLength);
            }
            switch (requestType) {
              case 0: // NBD_CMD_READ
                os.writeInt(0x67446698); // MAGIC
                os.writeInt(0); // OK
                os.write(handle);
                os.write(storage, offset.intValue(), requestLength.intValue());
                os.flush();
                System.out.println("Read: " + offset + ", " + requestLength);
                break;
              case 1: // NBD_CMD_WRITE
                in.readFully(storage, offset.intValue(), requestLength.intValue());
                writeReplyHeader(os, handle);
                System.out.println("Write: " + offset + ", " + requestLength);
                break;
              case 2: // NBD_CMD_DISC
                accept.close();
                break;
              case 3: // NBD_CMD_FLUSH
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

  private static void writeReplyHeader(DataOutputStream os, byte[] handle) throws IOException {
    os.writeInt(0x67446698);
    os.writeInt(0); // OK
    os.write(handle);
    os.flush();
  }
}
