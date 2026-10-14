// Regenerates read_values.bin, read_aggregates.bin and read_batches.bin with the platform's own
// DatapointFrameWriter.relayStream, the code that answers POST /timeseries/data/list/binary: Arrow
// streams compressed in-band, as ClickHouse answers, relayed into compression 2 frames. Run from a
// platform checkout that has built datahub-api-model, with arrow-format, flatbuffers-java,
// zero-allocation-hashing, zstd-jni and slf4j-api on the classpath:
//
//   java -cp "<datahub-api-model/build/classes/java/main>:<those jars>" \
//       GenerateReadFixtures.java src/timeseries/fixtures
//
// The Rust tests in binary_read.rs assert on the exact values written here. read_*_zstd.bin are
// the same series as compression 1 frames, written by the writer's former `wrap`, which is gone.
import ai.intellistream.datahub.api.binary.*;
import ai.intellistream.datahub.api.binary.ArrowIpc.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Writes binary-read response bodies with the platform's own writer, for the Rust reader to parse. */
public class GenerateReadFixtures {
    static final long T0 = 1_700_000_000_000L;
    static final PayloadCodec CODEC = new ZstdPayloadCodec(3);

    /** An ingest frame's batches as ClickHouse would answer them, relayed the way the API answers a read. */
    static byte[] response(DatapointValueType type, long id, String ext, DatapointFrameWriter w) throws Exception {
        byte[] ingest = w.build(CODEC);
        int dirLen = ByteBuffer.wrap(ingest).order(ByteOrder.LITTLE_ENDIAN).getInt(16);
        int payLen = ByteBuffer.wrap(ingest).order(ByteOrder.LITTLE_ENDIAN).getInt(20);
        int rawLen = ByteBuffer.wrap(ingest).order(ByteOrder.LITTLE_ENDIAN).getInt(24);
        byte[] payload = java.util.Arrays.copyOfRange(ingest, 28 + dirLen, 28 + dirLen + payLen);
        byte[] raw = CODEC.decompress(payload, rawLen);
        List<BatchData> data = new ArrayList<>();
        for (Batch b : ArrowIpc.readStream(ArrowSchemaCanon.of(type), raw, 0, 100_000)) {
            data.add(b.toData());
        }
        return relay(type, false, id, ext, ArrowIpc.writeStream(ArrowSchemaCanon.of(type), data, CODEC));
    }

    static byte[] relay(DatapointValueType type, boolean aggregates, long id, String ext, byte[] stream) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatapointFrameWriter.relayStream(type, aggregates, id, ext, new ByteArrayInputStream(stream), CODEC, out);
        return out.toByteArray();
    }

    /** {@code rows} rows of series {@code id} from {@code from}, a second apart, values from {@code values}. */
    static BatchData batch(long id, int from, int rows, java.util.function.IntFunction<ColumnData> values) {
        long[] ids = new long[rows];
        long[] stamps = new long[rows];
        for (int r = 0; r < rows; r++) {
            ids[r] = id;
            stamps[r] = T0 + (from + r) * 1000L;
        }
        return new BatchData(rows, new ColumnData[]{
                new ColumnData(null, 0, null, longs(ids)),
                new ColumnData(null, 0, null, longs(stamps)),
                values.apply(from),
        });
    }

    static ByteBuffer longs(long... v) {
        ByteBuffer b = ByteBuffer.allocate(v.length * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (long x : v) b.putLong(x);
        return b.flip();
    }

    static ByteBuffer doubles(double... v) {
        ByteBuffer b = ByteBuffer.allocate(v.length * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (double x : v) b.putDouble(x);
        return b.flip();
    }

    public static void main(String[] args) throws Exception {
        Path out = Path.of(args[0]);
        ByteArrayOutputStream values = new ByteArrayOutputStream();

        var bigint = DatapointFrameWriter.forType(DatapointValueType.BIGINT).series(11, "rd_bigint");
        bigint.addBigint(11, T0 + 2000, -9_000_000_000L);
        bigint.addBigint(11, T0, 1);
        bigint.addBigint(11, T0 + 1000, 42);
        values.writeBytes(response(DatapointValueType.BIGINT, 11, "rd_bigint", bigint));

        var flt = DatapointFrameWriter.forType(DatapointValueType.FLOAT).series(12, "rd_float");
        flt.addFloat(12, T0, 1.5);
        flt.addFloat(12, T0 + 1000, -2.25);
        values.writeBytes(response(DatapointValueType.FLOAT, 12, "rd_float", flt));

        var f32 = DatapointFrameWriter.forType(DatapointValueType.FLOAT32).series(13, "rd_float32");
        f32.addFloat32(13, T0, 0.5f);
        values.writeBytes(response(DatapointValueType.FLOAT32, 13, "rd_float32", f32));

        var numeric = DatapointFrameWriter.forType(DatapointValueType.NUMERIC).series(14, "rd_numeric");
        numeric.addNumeric(14, T0, new BigDecimal("123456789012.345678"));
        numeric.addNumeric(14, T0 + 1000, new BigDecimal("-0.000001"));
        values.writeBytes(response(DatapointValueType.NUMERIC, 14, "rd_numeric", numeric));

        var dec32 = DatapointFrameWriter.forType(DatapointValueType.DECIMAL32).series(15, "rd_decimal32");
        dec32.addDecimal32(15, T0, new BigDecimal("12345.6789"));
        values.writeBytes(response(DatapointValueType.DECIMAL32, 15, "rd_decimal32", dec32));

        var text = DatapointFrameWriter.forType(DatapointValueType.TEXT).series(16, "rd_text");
        text.addText(16, T0, "open");
        text.addText(16, T0 + 1000, "blåbær");
        values.writeBytes(response(DatapointValueType.TEXT, 16, "rd_text", text));

        var mixed = DatapointFrameWriter.forType(DatapointValueType.MIXED).series(17, "rd_mixed");
        mixed.addMixed(17, T0, "3.5");
        mixed.addMixed(17, T0 + 1000, "fault");
        mixed.addMixed(17, T0 + 2000, "-7");
        values.writeBytes(response(DatapointValueType.MIXED, 17, "rd_mixed", mixed));

        // Only numbers: the API leaves the validity bitmap out of a column with no nulls.
        var mixedNumbers = DatapointFrameWriter.forType(DatapointValueType.MIXED).series(18, "rd_mixed_numbers");
        mixedNumbers.addMixed(18, T0, "1");
        mixedNumbers.addMixed(18, T0 + 1000, "2");
        values.writeBytes(response(DatapointValueType.MIXED, 18, "rd_mixed_numbers", mixedNumbers));

        values.writeBytes(FrameLimits.END_OF_RESPONSE);
        Files.write(out.resolve("read_values.bin"), values.toByteArray());

        ByteArrayOutputStream aggregates = new ByteArrayOutputStream();
        BatchData buckets = new BatchData(3, new ColumnData[]{
                new ColumnData(null, 0, null, longs(21, 21, 21)),
                new ColumnData(null, 0, null, longs(T0, T0 + 3_600_000, T0 + 7_200_000)),
                new ColumnData(null, 0, null, doubles(1.5, 2.5, 3.5)),
                new ColumnData(null, 0, null, doubles(1, 2, 3)),
                new ColumnData(null, 0, null, doubles(2, 3, 4)),
                new ColumnData(null, 0, null, doubles(6, 10, 14)),
        });
        aggregates.writeBytes(relay(DatapointValueType.FLOAT, true, 21, "rd_aggregates",
                ArrowIpc.writeStream(ArrowSchemaCanon.aggregates(), List.of(buckets), CODEC)));
        aggregates.writeBytes(FrameLimits.END_OF_RESPONSE);
        Files.write(out.resolve("read_aggregates.bin"), aggregates.toByteArray());

        // Batches big enough that zstd shrinks their buffers, several to a frame, and one stream
        // that came uncompressed and longer than a frame, which the relay cuts and compresses.
        ByteArrayOutputStream batches = new ByteArrayOutputStream();
        int n = 3000;
        List<BatchData> floats = List.of(
                batch(31, 0, n, from -> new ColumnData(null, 0, null, doubles(java.util.stream.IntStream.range(from, from + n).mapToDouble(i -> i * 0.5).toArray()))),
                batch(31, n, n, from -> new ColumnData(null, 0, null, doubles(java.util.stream.IntStream.range(from, from + n).mapToDouble(i -> i * 0.5).toArray()))));
        batches.writeBytes(relay(DatapointValueType.FLOAT, false, 31, "rd_float_batches",
                ArrowIpc.writeStream(ArrowSchemaCanon.of(DatapointValueType.FLOAT), floats, CODEC)));
        int t = 12_000;
        BatchData texts = batch(32, 0, t, from -> {
            ByteBuffer offsets = ByteBuffer.allocate((t + 1) * 4).order(ByteOrder.LITTLE_ENDIAN);
            ByteArrayOutputStream chars = new ByteArrayOutputStream();
            for (int i = 0; i < t; i++) {
                offsets.putInt(chars.size());
                chars.writeBytes(("v" + (i % 10)).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            offsets.putInt(chars.size());
            return new ColumnData(null, 0, offsets.flip(), ByteBuffer.wrap(chars.toByteArray()));
        });
        batches.writeBytes(relay(DatapointValueType.TEXT, false, 32, "rd_text_cut",
                ArrowIpc.writeStream(ArrowSchemaCanon.of(DatapointValueType.TEXT), List.of(texts))));
        batches.writeBytes(FrameLimits.END_OF_RESPONSE);
        Files.write(out.resolve("read_batches.bin"), batches.toByteArray());
    }
}
