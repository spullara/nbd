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

import com.google.common.base.Charsets;
import com.sampullara.cli.Argument;

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

  @Argument(alias = "p", description = "The server port to listen on for connections")
  private static Integer port = 10809;

  public static void main(String[] args) throws IOException {
    ExecutorService es = Executors.newCachedThreadPool();
    log.info("Listening for nbd-client connections");
    ServerSocket ss = new ServerSocket(port);
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

          // TODO: interpret the client flags.
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
