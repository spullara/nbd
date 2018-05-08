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

import com.apple.foundationdb.FDB;
import com.google.common.primitives.Longs;
import com.sampullara.cli.Argument;
import nbdfdb.FDBArray;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static nbdfdb.NBD.SIZE_KEY;

public class CreateCommand implements Runnable {
  FDB fdb = FDB.selectAPIVersion(510);

  @Argument(alias = "n", description = "Name of the volume to create", required = true)
  private String exportName;

  @Argument(alias = "s", description = "Size in bytes of the volume, can use K, M, G, or T units.", required = true)
  private String size;

  @Argument(alias = "b", description = "Block size of the volume")
  private Integer blockSize = 512;

  enum Unit {
    K(1_000L),
    M(1_000_000L),
    G(1_000_000_000L),
    T(1_000_000_000_000L),;

    final long factor;

    Unit(long factor) {
      this.factor = factor;
    }
  }

  @Override
  public void run() {
    Pattern pattern = Pattern.compile("([0-9]+)(k|K|m|M|g|G|t|T)?(b|B)?");
    Matcher matcher = pattern.matcher(size);
    if (matcher.matches()) {
      long value = Long.parseLong(matcher.group(1));
      String unitGroup = matcher.group(2);
      if (unitGroup != null) {
        Unit unit = Unit.valueOf(unitGroup.toUpperCase());
        value *= unit.factor;
      }
      FDBArray fdbArray = FDBArray.create(fdb.open(), exportName, blockSize);
      fdbArray.setMetadata(SIZE_KEY, Longs.toByteArray(value));
      System.out.println("Successfully created '" + exportName + "'");
    } else {
      System.err.println("Invalid size: " + size);
    }
  }
}
