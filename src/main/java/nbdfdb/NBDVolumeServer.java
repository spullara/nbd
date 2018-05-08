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

import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import nbdfdb.NBD.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

import static nbdfdb.NBD.*;

/**
 * Created by sam on 11/9/14.
 */
public class NBDVolumeServer implements Runnable {

  private final Logger log;

  private final DataInputStream in;
  private final DataOutputStream out;
  private final String exportName;
  private final Storage storage;

  public NBDVolumeServer(String exportName, DataInputStream in, DataOutputStream out) throws IOException {
    this.exportName = exportName;
    log = Logger.getLogger("NDB: " + exportName);
    storage = new FDBStorage(exportName);
    log.info("Mounting " + exportName + " of size " + storage.size());
    storage.connect();
    this.in = in;
    this.out = out;
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
      out.writeLong(storage.size());
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
            log.info("Reading " + buffer.length + " from " + offset);
            storage.read(buffer, offset.intValue()).thenApply($ -> {
              synchronized (out) {
                try {
                  out.write(NBD_REPLY_MAGIC_BYTES);
                  out.write(NBD_OK_BYTES);
                  out.writeLong(handle);
                  out.write(buffer);
                  out.flush();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
              return null;
            });
            break;
          }
          case WRITE: {
            byte[] buffer = new byte[requestLength.intValue()];
            in.readFully(buffer);
            log.info("Writing " + buffer.length + " to " + offset);
            storage.write(buffer, offset.intValue()).thenApply($ -> {
              try {
                writeReplyHeaderAndFlush(handle);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return null;
            });
            break;
          }
          case DISCONNECT:
            log.info("Disconnecting " + exportName);
            storage.disconnect();
            return;
          case FLUSH:
            log.info("Flushing");
            long start = System.currentTimeMillis();
            storage.flush().thenApply($ -> {
              try {
                writeReplyHeaderAndFlush(handle);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              log.info("Flush complete: " + (System.currentTimeMillis() - start) + "ms");
              return null;
            });
            break;
          case TRIM:
            log.warning("Trim unimplemented");
            writeReplyHeaderAndFlush(handle);
            break;
          case CACHE:
            log.warning("Cache unimplemented");
            break;
        }
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "Unmounting volume " + exportName, e);
    }
  }
}
