import gnu.trove.list.array.TLongArrayList;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Fedor Podtelkin
 */
public class SimpleSNA3 {
    private static final double LIMIT = 4000000;
    private static final int THREADS = 4;

    private long tm = System.currentTimeMillis();
    private final Object lock = new Object();
    private Index matrix;
    private Index invert;

    public static void main(String[] args) throws Exception {
        long startTm = System.currentTimeMillis();
        SimpleSNA3 sna = new SimpleSNA3();
        sna.loadInput();
        sna.calc();
        System.out.println("Total time: " + (System.currentTimeMillis() - startTm)/1000 + " sec.");
    }

    private void loadInput() throws Exception {
        matrix = new Index("data/matrix.dump", true);
        invert = new Index("data/invert.dump", false);
        milestone("loadInput");
    }

    public void calc() throws Exception {
        PrintWriter res = new PrintWriter(new BufferedWriter(new FileWriter("result.txt"), 100000));
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        final int[] weight = new int[10000];
        for (int x = 0; x < weight.length; x++)
            weight[x] = (int) Math.round(100d / Math.pow(x + 10, 1 / 3d) - 6);  // 100/(x+10)^(1/3)-6

        for (int tid = 0; tid < THREADS; tid++) {
            final int threadId = tid;
            executor.submit(() -> {
                TLongArrayList friendsFriends = new TLongArrayList(10000000);
                TLongArrayList uniqCount = new TLongArrayList(10000000);
                for (int i = 0; i < matrix.size; i++) {
                    int id = matrix.ids[i];
                    if (id % THREADS != threadId) {
                        continue;
                    }

                    friendsFriends.resetQuick();
                    for (int j = 0; j < matrix.len[i]; j++) {
                        int fr = matrix.data[matrix.offset[i] + j];
                        int k = invert.index(fr);
                        int L = invert.len[k];

                        for (int g = 0; g < invert.len[k]; g++) {
                            long a = invert.data[invert.offset[k] + g];
                            long v = (a << 32) + weight[L];
                            friendsFriends.add(v);
                        }
                    }
                    friendsFriends.sort();

                    uniqCount.resetQuick();
                    long last = -1;
                    for (int j = 0, prevId = -1; j < friendsFriends.size(); j++) {
                        long curr = friendsFriends.get(j);
                        int currId = (int) (curr >> 32);
                        int currCount = (int) (curr & _MASK);
                        if (currId == prevId) {
                            uniqCount.setQuick(uniqCount.size() - 1, last += currCount * _COUNT);
                        } else {
                            uniqCount.add(last = currCount * _COUNT + currId);
                        }
                        prevId = currId;
                    }
                    uniqCount.sort();
                    uniqCount.reverse();

                    int count = (int) Math.round(matrix.len[i] * LIMIT / matrix.edges7Mod11);

                    synchronized (lock) {
                        int g = 0;
                        for (int j = 0; j < uniqCount.size() && g < count; j++) {
                            long val = uniqCount.get(j);
                            int a = (int) (val & _MASK);
                            if (a != id && Arrays.binarySearch(matrix.data, matrix.offset[i], matrix.offset[i] + matrix.len[i], a) < 0) {
                                if (g == 0) res.print(id);
                                res.print(" " + a);
                                g++;
                            }
                        }
                        if (g > 0) res.println();
                    }
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(7, TimeUnit.DAYS);
        res.close();
        milestone("calc");
    }

    private static final Long _COUNT = 0x100000000L;
    private static final Long _MASK = _COUNT - 1;

    private void milestone(String key) throws Exception {
        System.out.println("[" + key + "]\tDONE\t" + (System.currentTimeMillis() - tm) + "ms");
        tm = System.currentTimeMillis();
    }

    private class Index {
        final int size;
        final int edges7Mod11;
        final int[] ids;
        final int[] offset;
        final short[] len;
        final int[] data;

        Index(String filename, boolean light) throws Exception {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(filename), 1000000));
            int size = in.readInt(), mem = in.readInt();
            int size7Mod11 = in.readInt(), mem7Mod11 = in.readInt();

            this.data = new int[light ? mem7Mod11 : mem];
            this.size = light ? size7Mod11 : size;
            this.edges7Mod11 = mem7Mod11;
            System.out.println("[create-index]: size = " + this.size + ", mem = " + data.length);
            this.ids = new int[this.size];
            this.offset = new int[this.size];
            this.len = new short[this.size];

            for (int i = 0, off = 0; i < this.size;) {
                ids[i] = in.readInt();
                int inc = !light || ids[i] % 11 == 7 ? 1 : 0;
                len[i] = in.readShort();
                offset[i] = off;
                for (int j = 0; j < len[i]; j++, off += inc) {
                    data[off] = in.readInt();
                }
                i += inc;
            }
            in.close();
        }
        int index(int id) {
            return Arrays.binarySearch(ids, id);
        }
    }
}