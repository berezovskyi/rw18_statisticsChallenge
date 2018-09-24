package rw2018.statistics.impl;

import java.io.File;

import rw2018.statistics.StatisticsDB;
import rw2018.statistics.TriplePosition;

/**
 * This is the class that will be executed during the evaluation!!
 * 
 * TODO implement this class
 */
public class StatisticsDBImpl implements StatisticsDB {

    @Override
  public void setUp(File statisticsDir, int numberOfChunks) {
    // TODO Auto-generated method stub

  }

  @Override
  public int getNumberOfChunks() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void incrementFrequency(long resourceId, int chunkNumber, TriplePosition triplePosition) {
    // TODO Auto-generated method stub

  }

  @Override
  public long getFrequency(long resourceId, int chunkNumber, TriplePosition triplePosition) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

}
