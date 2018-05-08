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

import com.sampullara.cli.Args;

public class NBDCLI {

  enum CommandType {
    CREATE(new CreateCommand()),
    DELETE(new DeleteCommand()),
    SNAPSHOT(new SnapshotCommand()),
    LIST(new ListCommand()),
    SERVER(new ServerCommand()),
    ;
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
      System.exit(0);
    } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
      e.printStackTrace();
      System.err.println("Usage: nbdfdb.cli.NBDCLI [command name]");
      for (CommandType commandType : CommandType.values()) {
        System.err.print(commandType.name().toLowerCase() + ": ");
        Args.usage(commandType.command);
      }
      System.exit(1);
    }
  }
}
