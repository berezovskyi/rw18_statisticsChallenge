package rw2018.statistics;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.jetbrains.annotations.NotNull;
import rw2018.statistics.impl.StatisticsDBBaseImpl;
import rw2018.statistics.impl.StatisticsMapDBImpl;
import rw2018.statistics.io.EncodedFileInputStream;
import rw2018.statistics.io.EncodingFileFormat;
import rw2018.statistics.io.Statement;

/**
 * This class demonstrates how the {@link StatisticsDB} is used.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Main {

  private static void collectStatistics(File statisticsDir, File[] chunks) {
    if (statisticsDir.exists() && !statisticsDir.isDirectory()) {
      throw new IllegalArgumentException(
              "The working directory " + statisticsDir.getAbsolutePath() + " is not a directory.");
    }
    if (!statisticsDir.exists()) {
      statisticsDir.mkdirs();
    }

    // TODO adjust to your implementation
    try (StatisticsDB statisticsDB = getStatisticsDB();) {
      statisticsDB.setUp(statisticsDir, chunks.length);

      int statsCounter = 1;

      for (int chunkI = 0; chunkI < chunks.length; chunkI++) {
        File chunk = chunks[chunkI];
        try (EncodedFileInputStream input = new EncodedFileInputStream(EncodingFileFormat.EEE,
                chunk);) {
          for (Statement stmt : input) {
            statisticsDB.incrementFrequency(stmt.getSubjectAsLong(), chunkI,
                    TriplePosition.SUBJECT);
            statisticsDB.incrementFrequency(stmt.getPropertyAsLong(), chunkI,
                    TriplePosition.PROPERTY);
            statisticsDB.incrementFrequency(stmt.getObjectAsLong(), chunkI, TriplePosition.OBJECT);
//            if (statsCounter % 5  == 0) {
//              System.out.println(statisticsDB.prettyPrint());
//              statsCounter = 1;
//            }
//            statsCounter++;
//            printStats(statisticsDB);

          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        printStats(statisticsDB);
      }
      printStatsFinal(statisticsDB);
    }
  }

  private static void printStatsFinal(final StatisticsDB statisticsDB) {
    System.out.println(statisticsDB.prettyPrint());
  }

  private static void printStats(final StatisticsDB statisticsDB) {
    System.out.println(".");
  }

  @NotNull
  private static StatisticsDB getStatisticsDB() {
    return new StatisticsMapDBImpl();
  }

  public static void main(String[] args) throws ParseException {
    Option help = new Option("h", "help", false, "print this help message");
    help.setRequired(false);

    Option input = Option.builder("i").longOpt("input").hasArg().argName("inputDirectory")
            .desc("the directory in which the encoded chunks are stored").required(true).build();

    Option working = Option.builder("w").longOpt("workingDir").hasArg().argName("workingDirectory")
            .desc("the working directory in which the statistics database will be persisted")
            .required(true).build();

    Options options = new Options();
    options.addOption(help);
    options.addOption(input);
    options.addOption(working);

    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cLine = parser.parse(options, args);

      if (cLine.hasOption("h")) {
        Main.printUsage(options);
        return;
      }

      File workingDir = new File(cLine.getOptionValue('w'));
      File inputDir = new File(cLine.getOptionValue('i'));

      File[] chunks = inputDir.listFiles();
      Arrays.sort(chunks);
      Main.collectStatistics(workingDir, chunks);

    } catch (ParseException e) {
      Main.printUsage(options);
      throw e;
    }
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("java " + Main.class + " [-h] -i <inputDir> -w <workingDir>", options);
  }

}
