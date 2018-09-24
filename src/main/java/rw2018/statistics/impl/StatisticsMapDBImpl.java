package rw2018.statistics.impl;

import java.io.File;
import java.util.concurrent.ConcurrentMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import rw2018.statistics.StatisticsDB;
import rw2018.statistics.TriplePosition;

/**
 * This is the class that will be executed during the evaluation!!
 * <p>
 * TODO implement this class
 */
public class StatisticsMapDBImpl implements StatisticsDB {

    private int bits;
    private int numberOfChunks;
    private ConcurrentMap<String, Long> map;
    private DB db;

    @Override
    public void setUp(File statisticsDir, int numberOfChunks) {
        // TODO Auto-generated method stub
        this.bits = chunkBits(numberOfChunks);
        this.numberOfChunks = numberOfChunks;
        db = DBMaker.memoryDB().make();
        this.map = db.hashMap("map", Serializer.STRING, Serializer.LONG_PACKED).createOrOpen();
    }

    int chunkBits(final int numberOfChunks) {
        return (int) Math.ceil(Math.log(numberOfChunks) / Math.log(2));
    }

    @Override
    public int getNumberOfChunks() {
        return numberOfChunks;
    }

    @Override
    public void incrementFrequency(long resourceId, int chunkNumber,
            TriplePosition triplePosition) {
        final String key = key(resourceId, chunkNumber, triplePosition);
        final Long aLong = map.getOrDefault(key, 0l);
        map.put(key, aLong + 1);
    }

    private String key(final long resourceId, final int chunkNumber,
            final TriplePosition triplePosition) {
        return String.format("%d.%d.%s", resourceId, chunkNumber, triplePosition);
    }

    @Override
    public long getFrequency(long resourceId, int chunkNumber, TriplePosition triplePosition) {
        final String key = key(resourceId, chunkNumber, triplePosition);
        return map.get(key);
    }

    @Override
    public void close() {
        db.close();
    }

}
