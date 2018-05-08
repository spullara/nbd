package nbdfdb.cli;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.sampullara.cli.Argument;
import nbdfdb.FDBArray;

public class SnapshotCommand implements Runnable {
  FDB fdb = FDB.selectAPIVersion(510);

  @Argument(alias = "n", description = "Name of the snapshot volume to create", required = true)
  private String exportName;

  @Argument(alias = "v", description = "Name of the volume to snapshot", required = true)
  private String volumeName;

  @Override
  public void run() {
    Database db = fdb.open();
    FDBArray volume = FDBArray.open(db, volumeName);
    FDBArray snapshot = volume.snapshot(exportName);
    System.out.println("Successfully snapshotted " + volumeName + " as " + exportName);
  }
}
