import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.hash.TByteHashSet;

import java.io.*;
import java.util.Arrays;
import java.util.Date;

/**
 * @author Fedor Podtelkin
 */
public class SimpleSNA5 {

    private static final double LIMIT = 50000000;
    private static final int THREADS = 24;

    private static final int OTHER_DETAILS_LINES = 64255433;
    private static final int MAX_FRIENDS = 10000;

    private static final int LEN = 10;
    private static final int P_ID = 0;
    private static final int P_COMMON_FRIENDS = 1;
    private static final int P_COMMON_RELATIVES = 2;
    private static final int P_COMMON_SCHOOL = 3;
    private static final int P_COMMON_WORK = 4;
    private static final int P_COMMON_UNIVERSITY = 5;
    private static final int P_COMMON_ARMY = 6;
    private static final int P_AA_SCORE = 7;
    private static final int P_FEDOR_SCORE = 8;
    private static final int P_FEDOR2_SCORE = 9;

    private static final byte R_LOVE = 1;
    private static final byte R_SPOUSE = 2;
    private static final byte R_PARENT = 3;
    private static final byte R_CHILD = 4;
    private static final byte R_BROTHER = 5;
    private static final byte R_UNCLE = 6;
    private static final byte R_RELATIVE = 7;
    private static final byte R_CLOSE_FRIEND = 8;
    private static final byte R_COLLEAGUE = 9;
    private static final byte R_SCHOOLMATE = 10;
    private static final byte R_NEPHEW = 11;
    private static final byte R_GRANDPARENT = 12;
    private static final byte R_GRANDCHILD = 13;
    private static final byte R_UNIVERSITY = 14;
    private static final byte R_ARMY = 15;
    private static final byte R_PARENT_IN_LAW = 16;
    private static final byte R_CHILD_IN_LAW = 17;
    private static final byte R_GODPARENT = 18;
    private static final byte R_GODCHILD = 19;
    private static final byte R_PLAYING_TOGETHER = 20;

    private static final TByteHashSet ANY_RELATIVE = new TByteHashSet(new byte[]{R_LOVE, R_SPOUSE, R_PARENT, R_CHILD, R_BROTHER, R_UNCLE, R_RELATIVE, R_NEPHEW, R_GRANDPARENT, R_GRANDCHILD});

    private long tm = System.currentTimeMillis();
    private final Object lock = new Object();
    private Index matrix;
    private Index invert;
    private TIntIntHashMap friendLen = new TIntIntHashMap(2*OTHER_DETAILS_LINES+5);

    public static void main(String[] args) throws Exception {
        long startTm = System.currentTimeMillis();
        SimpleSNA5 sna = new SimpleSNA5();
        sna.loadOtherDetails();
        sna.loadInput();
        sna.calc();
        System.out.println("Total time: " + (System.currentTimeMillis() - startTm) / 1000 + " sec.");
        Thread.sleep(2000);
    }

    private void loadInput() throws Exception {
        matrix = new Index("data/matrixWT.dump", true);
        invert = new Index("data/invertWT.dump", false);
        milestone("loadInput");
    }

    private void loadOtherDetails() throws Exception {
        BufferedReader in = new BufferedReader(new FileReader("otherDetails/part-00000"), 1000000);
        int lines = 0;
        while (true) {
            String s = in.readLine();
            if (s == null) {
                break;
            }
            lines++;
            int c1 = s.indexOf(',');
            int cL = s.lastIndexOf(',');
            friendLen.put(Integer.parseInt(s.substring(0, c1)), Integer.parseInt(s.substring(cL+1)));
        }
        in.close();
        System.out.println("LINES " + lines);
        milestone("loadOtherDetails");
    }

    private boolean isOK(int id) {
        return (id % 11 == 7) || (id % 11 == 6) || (id % 11 == 5);
    }

    public void calc() throws Exception {
        PrintWriter res = new PrintWriter(new BufferedWriter(new FileWriter("data/counters.txt"), 1000000));

        final double[] weightAAScore = new double[MAX_FRIENDS];
        final double[] weightFedorScore = new double[MAX_FRIENDS];
        final double[] weightFedor2Score = new double[MAX_FRIENDS];
        for (int x = 0; x < MAX_FRIENDS; x++) {
            weightAAScore[x] = 1d / Math.log(x);
            weightFedorScore[x] = Math.max(0, 100d / Math.pow(x + 5, 1 / 3d) - 7);  // 100/(x+5)^(1/3)-7
            weightFedor2Score[x] = Math.max(0, 10d/lg2((x + 30) / 200d + 1)-5);  // 10/log((x+30)/200+1;2)-5
        }

        Thread[] threads = new Thread[THREADS];
        for (int tid = 0; tid < THREADS; tid++) {
            final int threadId = tid;
            threads[tid] = new Thread(() -> {
                System.out.println("MATRIX " + matrix.size);

                final int MAGIC = 5000000;
                final TIntIntHashMap p_counters = new TIntIntHashMap(2*MAGIC);
                int[] indexes = new int[MAGIC];
                final double[] data_counters = new double[LEN * MAGIC];
                int data_pointer = 0;

                int ping = matrix.size / 100;
                for (int i = 0; i < matrix.size; i++) {
                    if (i > 0 && i % ping == 0) {
                        System.out.println(new Date() + " Thread#" + threadId + " " + i + " " + i/ping + "%");
                    }
                    int id = matrix.ids[i];
                    if (!isOK(id)) {
                        continue;
                    }
                    if (id % THREADS != threadId) {
                        continue;
                    }

                    data_pointer = 0;
                    p_counters.clear();
                    for (int j = 0; j < matrix.len[i]; j++) {
                        int fr = matrix.data[matrix.offset[i] + j];
                        byte typeFr = matrix.type[matrix.offset[i] + j];
                        int k = invert.index(fr);
                        int L = Math.max(friendLen.get(k), invert.len[k]);

                        double wAAScore = weightAAScore[L];
                        double wFedor = weightFedorScore[L];
                        double wFedor2 = weightFedor2Score[L];

                        for (int g = 0; g < invert.len[k]; g++) {
                            int key = invert.data[invert.offset[k] + g];
                            byte typeKey = invert.type[invert.offset[k] + g];

                            int pointer;
                            if (p_counters.containsKey(key)) {
                                pointer = p_counters.get(key);
                            } else {
                                pointer = data_pointer;
                                Arrays.fill(data_counters, pointer, pointer + LEN, 0d);
                                data_pointer += LEN;
                                data_counters[pointer + P_ID] = key;
                                p_counters.put(key, pointer);
                            }

                            data_counters[pointer + P_COMMON_FRIENDS]++;
                            if (ANY_RELATIVE.contains(typeFr) && ANY_RELATIVE.contains(typeKey)) {
                                data_counters[pointer + P_COMMON_RELATIVES]++;
                            }
                            if (typeFr == R_SCHOOLMATE && typeKey == R_SCHOOLMATE) {
                                data_counters[pointer + P_COMMON_SCHOOL]++;
                            }
                            if (typeFr == R_COLLEAGUE && typeKey == R_COLLEAGUE) {
                                data_counters[pointer + P_COMMON_WORK]++;
                            }
                            if (typeFr == R_UNIVERSITY && typeKey == R_UNIVERSITY) {
                                data_counters[pointer + P_COMMON_UNIVERSITY]++;
                            }
                            if (typeFr == R_ARMY && typeKey == R_ARMY) {
                                data_counters[pointer + P_COMMON_ARMY]++;
                            }

                            data_counters[pointer + P_AA_SCORE] += wAAScore;
                            data_counters[pointer + P_FEDOR_SCORE] += wFedor;
                            data_counters[pointer + P_FEDOR2_SCORE] += wFedor2;
                        }
                    }

                    int size = p_counters.size();
                    indexes = p_counters.keys(indexes);
                    new Sorter(p_counters, indexes, data_counters).qsort(0, size - 1);

                    int count = (int) Math.round(matrix.len[i] * LIMIT / matrix.edges7Mod11);

                    synchronized (lock) {
                        int g = 0;
                        for (int j = 0; j < size && g < count; j++) {
                            int pointer = p_counters.get(indexes[j]);
                            int a = (int) data_counters[pointer + P_ID];
                            if (a != id) {
                                res.println(id + " " + a + " " + (int)data_counters[pointer + 1] + " " + (int)data_counters[pointer + 2] +
                                        " " + (int)data_counters[pointer + 3] + " " + (int)data_counters[pointer + 4] + " " + (int)data_counters[pointer + 5] + " " + (int)data_counters[pointer + 6]
                                        + " " + data_counters[pointer + 7] + " " + data_counters[pointer + 8] + " " + data_counters[pointer + 9]);
                                g++;
                            }
                        }
                    }
                }
            });
            threads[tid].start();
        }
        Thread.sleep(2000);
        for (Thread thread : threads) {
            thread.join();
        }
        res.close();
        milestone("calc");
    }

    private class Sorter {
        final TIntIntHashMap p_counters;
        final int[] indexes;
        final double[] data_counters;

        public Sorter(TIntIntHashMap p_counters, int[] indexes, double[] data_counters) {
            this.p_counters = p_counters;
            this.indexes = indexes;
            this.data_counters = data_counters;
        }

        double val(int index) {
            int i1 = indexes[index];
            int i2 = p_counters.get(i1);
            return data_counters[i2 + P_FEDOR_SCORE];
        }

        void qsort(int low, int high) {
            if (low >= high) {
                return;
            }
            int i = low;
            int j = high;
            double x = val(low+(high-low)/2);
            do {
                while(val(i) > x) ++i;
                while(val(j) < x) --j;
                if (i <= j) {
                    int tmp = indexes[i];  indexes[i] = indexes[j];       indexes[j] = tmp;
                    i++; j--;
                }
            } while(i <= j);
            if (low < j) qsort(low, j);
            if (i < high) qsort(i, high);
        }
    }


    private void milestone(String key) throws Exception {
        System.out.println("[" + key + "]\tDONE\t" + (System.currentTimeMillis() - tm) + "ms");
        tm = System.currentTimeMillis();
    }

    private class Index {
        int size;
        final int edges7Mod11;
        int[] ids;
        int[] offset;
        short[] len;
        int[] data;
        byte[] type;

        Index(String filename, boolean checkIsOk) throws Exception {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(filename), 1000000));
            int size = in.readInt(), mem = in.readInt();
            int size7Mod11 = in.readInt(), mem7Mod11 = in.readInt();

            this.data = new int[mem];
            this.type = new byte[mem];
            this.size = size;
            this.edges7Mod11 = mem7Mod11;
            System.out.println("[create-index]: size = " + this.size + ", mem = " + data.length);
            this.ids = new int[this.size];
            this.offset = new int[this.size];
            this.len = new short[this.size];

            int i = 0, off = 0;
            for (int q = 0; q < this.size; q++) {
                ids[i] = in.readInt();
                int inc = !checkIsOk || isOK(ids[i]) ? 1 : 0;
                len[i] = in.readShort();
                offset[i] = off;
                for (int j = 0; j < len[i]; j++, off += inc) {
                    data[off] = in.readInt();
                    type[off] = in.readByte();
                }
                i += inc;
            }

            if (i < size) {
                System.out.println("Size cut-off " + i);
                this.size = i;
                this.ids = Arrays.copyOf(this.ids, this.size);
                this.offset = Arrays.copyOf(this.offset, this.size);
                this.len = Arrays.copyOf(this.len, this.size);
            }
            if (off < mem) {
                System.out.println("Mem cut-off " + off);
                this.data = Arrays.copyOf(this.data, off);
                this.type = Arrays.copyOf(this.type, off);
            }

            in.close();
        }
        int index(int id) {
            return Arrays.binarySearch(ids, id);
        }
    }

    private static double lg2(double x) {
        return Math.log(x) / Math.log(2);
    }
}