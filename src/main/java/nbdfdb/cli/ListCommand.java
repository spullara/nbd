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
import com.apple.foundationdb.directory.DirectoryLayer;
import nbdfdb.FDBStorage;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;

public class ListCommand implements Runnable {
  FDB fdb = FDB.selectAPIVersion(510);

  @Override
  public void run() {
    Database db = fdb.open();
    List<String> exportNames = null;
    try {
      exportNames = DirectoryLayer.getDefault().list(db, singletonList("com.sampullara.fdb.array")).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    for (String exportName : exportNames) {
      FDBStorage fdbStorage = new FDBStorage(exportName);
      System.out.printf("%s: %d/%d %2.1f\n", exportName, fdbStorage.usage(), fdbStorage.size(), (double)fdbStorage.usage()/fdbStorage.size()*100);
    }
  }
}
