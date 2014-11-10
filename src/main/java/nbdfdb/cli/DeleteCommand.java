package nbdfdb.cli;

import com.foundationdb.FDB;
import com.sampullara.cli.Argument;
import com.sampullara.fdb.FDBArray;

/**
 * Created by sam on 11/9/14.
 */
public class DeleteCommand implements Runnable {

  FDB fdb = FDB.selectAPIVersion(200);

  @Argument(alias = "n", description = "Name of the volume to create", required = true)
  private String exportName;

  @Override
  public void run() {
    FDBArray fdbArray = FDBArray.open(fdb.open(), exportName);
    fdbArray.delete();
    System.out.println("Deleted volume " + exportName);
  }
}
