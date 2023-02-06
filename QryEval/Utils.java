/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;

/**
 *  Miscellaneous utilities.
 */
public class Utils {

  /**
   *  Run an external process.
   *  @param processName a name to display if the process fails
   *  @param parameters an array of strings used to construct the commandline
   *  @throws Exception the external process failed.
   */
  public static void runExternalProcess (String processName, String[] parameters)
    throws Exception {

      Process cmdProc = Runtime.getRuntime().exec(parameters);

      // Consume stdout and stderr.  THIS IS REQUIRED, otherwise the
      // buffers may fill, causing the program to stall.  Echoing
      // stdout and saving stderr (in case an exception is necessary)
      // is optional.

      BufferedReader stdoutReader =
	new BufferedReader(new InputStreamReader(cmdProc.getInputStream()));

      String line;
      while ((line = stdoutReader.readLine()) != null) {
	System.out.println (line);
      }

      String errorOut = "";
      BufferedReader stderrReader =
	new BufferedReader(new InputStreamReader(cmdProc.getErrorStream()));
      while ((line = stderrReader.readLine()) != null) {
	errorOut += line;
      }

      // Throw an exception if the process has problems.  This is
      // ugly, but there isn't any way to recover from problems.

      int retValue = cmdProc.waitFor();
      if (retValue != 0) {
	throw new Exception(processName + " crashed: " + errorOut);
      }
  }

}
