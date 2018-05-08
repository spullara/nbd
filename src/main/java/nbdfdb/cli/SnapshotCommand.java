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
