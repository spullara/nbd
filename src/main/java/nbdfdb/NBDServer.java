package nbdfdb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;
import com.sampullara.fdb.FDBArray;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static nbdfdb.NBD.INIT_PASSWD;
import static nbdfdb.NBD.NBD_FLAG_HAS_FLAGS;
import static nbdfdb.NBD.NBD_OPT_EXPORT_NAME;
import static nbdfdb.NBD.OPTS_MAGIC_BYTES;

public class NBDServer {

  public static void main(String[] args) throws IOException {
    Database db = FDB.selectAPIVersion(200).open();
    ExecutorService es = Executors.newCachedThreadPool();
    ServerSocket ss = new ServerSocket(10809);
    while (true) {
      Socket accept = ss.accept();
      es.submit(() -> {
        DataInputStream in = new DataInputStream(accept.getInputStream());
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(accept.getOutputStream()));
        out.write(INIT_PASSWD);
        out.write(OPTS_MAGIC_BYTES);
        out.writeShort(NBD_FLAG_HAS_FLAGS);
        out.flush();

        int clientFlags = in.readInt();
        long magic = in.readLong();
        int opt = in.readInt();
        if (opt != NBD_OPT_EXPORT_NAME) {
          throw new RuntimeException("We support only EXPORT options");
        }
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        String exportName = new String(bytes, Charsets.UTF_8);
        FDBArray fdbArray = FDBArray.open(db, exportName);
        byte[] size = fdbArray.getMetadata("size".getBytes());
        if (size == null) {
          throw new IllegalArgumentException("Size of volume not configured");
        }
        return new NBDVolumeServer(accept, fdbArray, Longs.fromByteArray(size), in, out);
      });
    }
  }
}
