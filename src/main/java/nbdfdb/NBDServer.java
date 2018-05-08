package nbdfdb;

import com.google.common.base.Charsets;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static nbdfdb.NBD.*;

public class NBDServer {

  private static Logger log = Logger.getLogger("NBD");

  public static void main(String[] args) throws IOException {
    ExecutorService es = Executors.newCachedThreadPool();
    log.info("Listening for nbd-client connections");
    ServerSocket ss = new ServerSocket(10809);
    while (true) {
      Socket accept = ss.accept();
      es.submit(() -> {
        try {
          InetSocketAddress remoteSocketAddress = (InetSocketAddress) accept.getRemoteSocketAddress();
          log.info("Client connected from: " + remoteSocketAddress.getAddress().getHostAddress());
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
          log.info("Connecting client to " + exportName);
          NBDVolumeServer nbdVolumeServer = new NBDVolumeServer(exportName, in, out);
          log.info("Volume mounted");
          nbdVolumeServer.run();
        } catch (Throwable e) {
          log.log(Level.SEVERE, "Failed to connect", e);
          try {
            accept.close();
          } catch (IOException e1) {
            e1.printStackTrace();
          }
        }
      });
    }
  }
}
