package nbdfdb;

import java.util.concurrent.CompletableFuture;

/**
 * Created by sam on 11/11/14.
 */
public interface Storage {
  void connect();

  void disconnect();

  CompletableFuture<Void> read(byte[] buffer, long offset);

  CompletableFuture<Void> write(byte[] buffer, long offset);

  CompletableFuture<Void> flush();

  long size();

  long usage();
}
