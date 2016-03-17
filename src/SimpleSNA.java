import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.StringTokenizer;

import gnu.trove.iterator.TIntShortIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntShortHashMap;

/**
 * @author Fedor Podtelkin
 */
public class SimpleSNA {
    private static final String INPUT_DIR = "graph/";    // папка, где лежат 16 распакованных входных файлов part-v008....
    private static final double LIMIT = 4000000;

    private long tm = System.currentTimeMillis();
    private int vertexes;
    private int vertexes7mod11;
    private int edges;
    private int edges7mod11;
    private TIntShortHashMap opposite = new TIntShortHashMap();
    private Index matrix;
    private Index invert;

    public static void main(String[] args) throws Exception {
        SimpleSNA sna = new SimpleSNA();
        sna.analyzeInput();
        sna.initMemory();
        sna.loadInput();
        sna.commonFriends();
    }

    private void analyzeInput() throws Exception {
        System.out.print("[analyzeInput]  ");
        int key = -1;
        for (File file : new File(INPUT_DIR).listFiles()) {
            BufferedReader in = new BufferedReader(new FileReader(file), 1000000);
            while (true) {
                String s = in.readLine();
                if (s == null) {
                    break;
                }
                StringTokenizer t = new StringTokenizer(s, " \t(),{}");
                for (int j = 0; t.hasMoreTokens(); j++) {
                    String a = t.nextToken();
                    if (j == 0) {
                        key = Integer.parseInt(a);
                        vertexes++;
                        if (key % 11 == 7) {
                            vertexes7mod11++;
                        }
                    } else if (j % 2 == 1) {
                        int v = Integer.parseInt(a);
                        opposite.adjustOrPutValue(v, (short)1, (short)1);
                        edges++;
                        if (key % 11 == 7) {
                            edges7mod11++;
                        }
                    }
                }
            }
            in.close();
            System.out.print(".");
        }
        System.out.println("\nvertexes = " + vertexes + "\nedges = " + edges + "\noppositeVertexes = " + opposite.size() + "\nvertexes7mod11 = " + vertexes7mod11 + "\nedges7mod11 = " + edges7mod11);
        milestone("analyzeInput");
    }

    private void initMemory() throws Exception {
        invert = new Index(edges, opposite.size());
        TIntShortIterator iter = opposite.iterator();
        for (int i = 0, shift = 0; i < invert.size; i++) {
            iter.advance();
            invert.ids[i] = iter.key();
            invert.offset[i] = shift;
            shift += iter.value();
        }
        iter = null;
        opposite = null;
        System.gc();
        invert.qsort(0, invert.size-1);

        matrix = new Index(edges7mod11, vertexes7mod11);
        milestone("initMemory");
    }

    private void loadInput() throws Exception {
        System.out.print("[loadInput]  ");
        int matrixIndex = 0, shift = 0, key = -1;
        for (File file : new File(INPUT_DIR).listFiles()) {
            BufferedReader in = new BufferedReader(new FileReader(file), 1000000);
            while (true) {
                String s = in.readLine();
                if (s == null) {
                    break;
                }
                StringTokenizer t = new StringTokenizer(s, " \t(),{}");
                for (int j = 0; t.hasMoreTokens(); j++) {
                    String a = t.nextToken();
                    if (j == 0) {
                        key = Integer.parseInt(a);
                        if (key % 11 == 7) {
                            matrix.ids[matrixIndex] = key;
                            matrix.offset[matrixIndex] = shift;
                            matrixIndex++;
                        }
                    } else if (j % 2 == 1) {
                        int v = Integer.parseInt(a);
                        if (key % 11 == 7) {
                            matrix.data[shift] = v;
                            matrix.len[matrixIndex-1]++;
                            shift++;
                        }

                        int k = invert.index(v);
                        invert.data[invert.offset[k]+invert.len[k]] = key;
                        invert.len[k]++;
                    }
                }
            }
            in.close();
            System.out.print(".");
        }
        System.out.println();
        milestone("loadInput");
    }

    public void commonFriends() throws Exception {
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("commonFriends.txt"), 100000));
        PrintWriter res = new PrintWriter(new BufferedWriter(new FileWriter("result.txt"), 100000));
        for (int i = 0; i < matrix.size; i++) {
            int id = matrix.ids[i];
            Arrays.sort(matrix.data, matrix.offset[i], matrix.offset[i]+matrix.len[i]);

            TIntArrayList friendsFriends = new TIntArrayList();
            for (int j = 0; j < matrix.len[i]; j++) {
                int fr = matrix.data[matrix.offset[i] + j];
                int k = invert.index(fr);
                friendsFriends.add(invert.data, invert.offset[k], invert.len[k]);
            }
            friendsFriends.sort();
            TLongArrayList uniqCount = new TLongArrayList();
            friendsFriends.forEach(val -> {
                int j = uniqCount.size() - 1;
                if (j < 0 || (uniqCount.get(j) & 0xFFFFFFFFL) != val) {
                    uniqCount.add(0x100000000L + val);
                } else {
                    uniqCount.set(j, uniqCount.get(j) + 0x100000000L);
                }
                return true;
            });
            uniqCount.sort();

            int count = (int)Math.round(matrix.len[i]*LIMIT/edges7mod11);
            out.print(id);
            boolean printed = false;

            for (int j = uniqCount.size()-1, k = 0; j >= 0 && k < Math.max(count, matrix.len[i])+30; j--, k++) {
                long val = uniqCount.get(j);
                int a = (int) (val & 0xFFFFFFFFL);
                if (a != id && Arrays.binarySearch(matrix.data, matrix.offset[i], matrix.offset[i]+matrix.len[i], a) < 0) {
                    out.print(" " + a + " " + (val >> 32));
                    if (count > 0) {
                        if (!printed) res.print(id);
                        printed = true;
                        res.print(" " + a);
                        count--;
                    }
                }
            }
            out.println();
            if (printed) res.println();
        }
        out.close();
        res.close();
        milestone("commonFriends");
    }

    private void milestone(String key) throws Exception {
        System.out.println("[" + key + "]\tDONE\t" + (System.currentTimeMillis() - tm) + "ms");
        tm = System.currentTimeMillis();
    }

    private class Index {
        final int size;
        final int[] ids;
        final int[] offset;
        final short[] len;
        final int[] data;

        Index(int mem, int size) {
            System.out.println("[create-index]: size = " + size + ", mem = " + mem);
            this.data = new int[mem];
            this.size = size;
            this.ids = new int[size];
            this.offset = new int[size];
            this.len = new short[size];
        }

        void qsort(int low, int high) {
            int i = low;
            int j = high;
            long x = ids[low+(high-low)/2];
            do {
                while(ids[i] < x) ++i;
                while(ids[j] > x) --j;
                if (i <= j) {
                    int tmp = ids[i];  ids[i] = ids[j];       ids[j] = tmp;
                    tmp = offset[i];   offset[i] = offset[j]; offset[j] = tmp;
                    short t2 = len[i]; len[i] = len[j];       len[j] = t2;
                    i++; j--;
                }
            } while(i <= j);
            if (low < j) qsort(low, j);
            if (i < high) qsort(i, high);
        }

        int index(int id) {
            return Arrays.binarySearch(ids, id);
        }
    }
}