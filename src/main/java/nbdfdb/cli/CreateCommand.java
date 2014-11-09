package nbdfdb.cli;

import com.foundationdb.FDB;
import com.google.common.primitives.Longs;
import com.sampullara.cli.Argument;
import com.sampullara.fdb.FDBArray;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static nbdfdb.NBD.SIZE_KEY;

public class CreateCommand implements Runnable {
  FDB fdb = FDB.selectAPIVersion(200);

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
