package nbdfdb.cli;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.directory.DirectoryLayer;
import nbdfdb.FDBStorage;

import java.util.Arrays;
import java.util.List;

public class ListCommand implements Runnable {
  FDB fdb = FDB.selectAPIVersion(200);

  @Override
  public void run() {
    Database db = fdb.open();
    List<String> exportNames = DirectoryLayer.getDefault().list(db, Arrays.asList("com.sampullara.fdb.array")).get();
    for (String exportName : exportNames) {
      FDBStorage fdbStorage = new FDBStorage(exportName);
      System.out.printf("%s: %d/%d %2.1f\n", exportName, fdbStorage.usage(), fdbStorage.size(), (double)fdbStorage.usage()/fdbStorage.size()*100);
    }
  }
}
