package rw2018.statistics.impl;

import java.io.File;
import java.util.Arrays;
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
        db = DBMaker.memoryDirectDB().make();
        final File file = new File(statisticsDir, "map.db");
        file.delete();
//        db = DBMaker.fileDB(file)
//                    .fileMmapEnableIfSupported()
//                    .concurrencyDisable()
//                    .fileMmapPreclearDisable()
//                    .cleanerHackEnable()
//                    .make();
        db.getStore().fileLoad();
        this.map = db.hashMap("map", Serializer.STRING, Serializer.LONG_PACKED).create();
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
        final String key = stringKey(resourceId, chunkNumber, triplePosition);
//        final Long key = baseKey(resourceId, chunkNumber, triplePosition);
        final Long aLong = map.getOrDefault(key, 0l);
        map.put(key, aLong + 1);
    }

//    private Long key(final long resourceId, final int chunkNumber,
//            final TriplePosition triplePosition) {
////        return stringKey(resourceId, chunkNumber, triplePosition);
////        return baseKey(resourceId, chunkNumber, triplePosition);
//    }

    private long baseKey(final long resourceId, final int chunkNumber,
            final TriplePosition triplePosition) {
        long sizeOfRow = 8 * numberOfChunks * 3;
        int indexOfTriplePosition = -1;
        for (int i = 0; i < getTriplePositions().length; i++) {
            if (getTriplePositions()[i] == triplePosition) {
                indexOfTriplePosition = i;
            }
        }
        if (indexOfTriplePosition == -1) {
            throw new IllegalArgumentException("The triple position " + triplePosition
                                                       + " is not supported. Supported triple positions are "
                                                       + Arrays.toString(getTriplePositions()) + ".");
        }
        final int idx = (indexOfTriplePosition * numberOfChunks) + chunkNumber;
        long offset = ((resourceId - 1) * sizeOfRow)
                + (idx * Long.BYTES);
        return offset;
    }

    private String stringKey(final long resourceId, final int chunkNumber,
            final TriplePosition triplePosition) {
        return String.format("%d.%d.%s", resourceId, chunkNumber, triplePosition);
    }

    @Override
    public long getFrequency(long resourceId, int chunkNumber, TriplePosition triplePosition) {
        final String key = stringKey(resourceId, chunkNumber, triplePosition);
//        final Long key = baseKey(resourceId, chunkNumber, triplePosition);
        return map.getOrDefault(key, 0l);
    }

    @Override
    public void close() {
        db.close();
    }

}
