package nbdfdb.cli;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import nbdfdb.FDBStorage;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ListCommand implements Runnable {
  FDB fdb = FDB.selectAPIVersion(510);

  @Override
  public void run() {
    Database db = fdb.open();
    List<String> exportNames = null;
    try {
      exportNames = DirectoryLayer.getDefault().list(db, Arrays.asList("com.sampullara.fdb.array")).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    for (String exportName : exportNames) {
      FDBStorage fdbStorage = new FDBStorage(exportName);
      System.out.printf("%s: %d/%d %2.1f\n", exportName, fdbStorage.usage(), fdbStorage.size(), (double)fdbStorage.usage()/fdbStorage.size()*100);
    }
  }
}
