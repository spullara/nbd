package nbdfdb.cli;

import nbdfdb.NBDServer;

import java.io.IOException;

public class ServerCommand implements Runnable {
  @Override
  public void run() {
    try {
      NBDServer.main(new String[0]);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
