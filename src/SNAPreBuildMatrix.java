import gnu.trove.iterator.TIntShortIterator;
import gnu.trove.map.hash.TIntShortHashMap;

import java.io.*;
import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * @author Fedor Podtelkin
 */
public class SNAPreBuildMatrix {
    private static final String INPUT_DIR = "graph/";    // папка, где лежат 16 распакованных входных файлов part-v008....

    private long tm = System.currentTimeMillis();
    private int vertexes;
    private int edges;
    private TIntShortHashMap opposite = new TIntShortHashMap();
    private Index matrix;
    private Index invert;

    public static void main(String[] args) throws Exception {
        SNAPreBuildMatrix sna = new SNAPreBuildMatrix();
        sna.analyzeInput();
        sna.initMemory();
        sna.loadInput();
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
                        vertexes++;
                    } else if (j % 2 == 1) {
                        int v = Integer.parseInt(a);
                        opposite.adjustOrPutValue(v, (short)1, (short)1);
                        edges++;
                    }
                }
            }
            in.close();
            System.out.print(".");
        }
        System.out.println("\nvertexes = " + vertexes + "\nedges = " + edges + "\noppositeVertexes = " + opposite.size());
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
        invert.qsort(0, invert.size - 1);

        matrix = new Index(edges, vertexes);
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
                int ixMatrix = -1, ixInvert = -1;
                for (int j = 0; t.hasMoreTokens(); j++) {
                    String a = t.nextToken();
                    if (j == 0) {
                        key = Integer.parseInt(a);
                        matrix.ids[matrixIndex] = key;
                        matrix.offset[matrixIndex] = shift;
                        matrixIndex++;
                    } else if (j % 2 == 1) {
                        int v = Integer.parseInt(a);
                        matrix.data[ixMatrix = shift] = v;
                        matrix.len[matrixIndex-1]++;
                        shift++;
                        int k = invert.index(v);
                        invert.data[ixInvert = invert.offset[k]+invert.len[k]] = key;
                        invert.len[k]++;
                    } else {
                        int v = Integer.parseInt(a);
                        byte ttype = 0;
                        for (byte f = 1; f <= 31; f++) {
                            if ((v & (1 << f)) != 0) {
                                ttype = f;
                                break;
                            }
                        }
                        matrix.type[ixMatrix] = ttype;
                        invert.type[ixInvert] = ttype;
                    }
                }
            }
            in.close();
            System.out.print(".");
        }
        System.out.println();
        milestone("loadInput");


        matrix.sortAll();
        invert.sortAll();
        matrix.dump("data/matrixWT.dump");
        invert.dump("data/invertWT.dump");
        milestone("dump");
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
        final byte[] type;

        Index(int mem, int size) {
            System.out.println("[create-index]: size = " + size + ", mem = " + mem);
            this.data = new int[mem];
            this.type = new byte[mem];
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

        void sortAll() {
            qsort(0, size-1);
//            for (int i = 0; i < size; i++) {
//                Arrays.sort(data, offset[i], offset[i] + len[i]);
//            }
        }

        void dump(String filename) throws Exception {
            int size7mod11 = 0;
            int edges7mod11 = 0;
            for (int i = 0; i < size; i++) {
                if (ids[i] % 11 == 7) {
                    size7mod11++;
                    edges7mod11 += len[i];
                }
            }
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename), 1000000));
            out.writeInt(size);
            out.writeInt(data.length);
            out.writeInt(size7mod11);
            out.writeInt(edges7mod11);
            for (int i = 0; i < size; i++) {
                out.writeInt(ids[i]);
                out.writeShort(len[i]);
                for (int j = 0; j < len[i]; j++) {
                    out.writeInt(data[offset[i] + j]);
                    out.writeByte(type[offset[i] + j]);
                }
            }
            out.close();
        }
    }
}