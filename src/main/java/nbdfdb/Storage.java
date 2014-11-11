package nbdfdb;

import com.foundationdb.async.Future;

/**
 * Created by sam on 11/11/14.
 */
public interface Storage {
  void connect();
  void disconnect();
  Future<Void> read(byte[] buffer, long offset);
  Future<Void> write(byte[] buffer, long offset);
  Future<Void> flush();
  long size();
  long usage();
}
