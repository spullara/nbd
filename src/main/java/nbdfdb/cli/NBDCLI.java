package nbdfdb.cli;

import com.sampullara.cli.Args;

public class NBDCLI {

  enum CommandType {
    CREATE(new CreateCommand()),
    DELETE(new DeleteCommand()),
    SNAPSHOT(new SnapshotCommand());
    private final Runnable command;

    CommandType(Runnable command) {
      this.command = command;
    }

    void run(String[] args) {
      Args.parseOrExit(command, args);
      command.run();
    }
  }

  public static void main(String[] args) {
    try {
      String arg = args[0];
      CommandType commandType = CommandType.valueOf(arg.toUpperCase());
      commandType.run(args);
    } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
      System.err.println("Usage: nbdfdb.cli.NBDCLI [command name]");
      for (CommandType commandType : CommandType.values()) {
        System.err.print(commandType.name().toLowerCase() + ": ");
        Args.usage(commandType.command);
      }
    }
  }
}
