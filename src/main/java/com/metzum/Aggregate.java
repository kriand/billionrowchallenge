package com.metzum;

import module java.base;
import sun.misc.Unsafe;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.reducing;

@SuppressWarnings("removal")
public class Aggregate {

    private static final int FNV_PRIME = 0x01000193;
    private static final int FNV_BASIS = 0x811c9dc5;

    final static Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void main(String[] args) {
        Path pathToFile = Paths.get(args[0]).toAbsolutePath();
        int concurrencyLevel = Runtime.getRuntime().availableProcessors();
        List<Future<Map<Integer, Result>>> futures = new ArrayList<>(concurrencyLevel);

        try (Arena arena = Arena.ofShared();
                FileChannel channel = FileChannel.open(pathToFile, StandardOpenOption.READ);
                ExecutorService executor = Executors.newFixedThreadPool(concurrencyLevel, r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                })) {
            long fileSize = Files.size(pathToFile);
            long address = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena).address();
            long approximateChunkSize = fileSize / concurrencyLevel;
            long previousChunkEnd = address;
            for (int i = 0; i < concurrencyLevel; i++) {
                long chunkStart = previousChunkEnd;
                long finalChunkSize;
                if (i == concurrencyLevel - 1) {
                    finalChunkSize = address + fileSize - chunkStart;
                } else {
                    long size = approximateChunkSize;
                    byte b;
                    do {
                        b = UNSAFE.getByte(chunkStart + size);
                        size++;
                    } while (b != '\n' && size < fileSize);
                    previousChunkEnd = chunkStart + size;
                    finalChunkSize = size;
                }

                Future<Map<Integer, Result>> future = executor.submit(() -> {
                    Map<Integer, Result> results = new HashMap<>(500);
                    long adr = chunkStart;
                    long chunkEnd = chunkStart + finalChunkSize;
                    int sign, scale, measurement, stationNameNumBytes, stationNameHash;
                    long stationNameStartAddress;
                    while (adr < chunkEnd) {
                        measurement = 0;
                        sign = 1;
                        scale = 10;
                        stationNameStartAddress = adr;
                        stationNameNumBytes = 0;
                        stationNameHash = FNV_BASIS;
                        while (true) {
                            byte b = UNSAFE.getByte(adr);
                            adr++;
                            if (b == ';') {
                                break;
                            }
                            stationNameHash ^= (b & 0xFF);
                            stationNameHash *= FNV_PRIME;
                            stationNameNumBytes++;
                        }
                        parseMeasurement:
                        while (true) {
                            byte b = UNSAFE.getByte(adr);
                            adr++;
                            switch (b) {
                                case '.' -> scale = 1;
                                case '-' -> sign = -1;
                                default -> {
                                    measurement = scale * (measurement + b - '0');
                                    if (scale == 1) { //If the scale is 1, we have processed the decimal value,
                                        adr++;               // the next character must be '\n'. Skip ahead one byte.
                                        break parseMeasurement;
                                    }
                                    else if (measurement >= 100) { //If the measurement is 10 degrees or more,
                                        scale = 1;                           // the next character must be '.'.
                                        adr++;                               // Skip ahead one byte.
                                    }
                                }
                            }
                        }
                        int m = sign * measurement;
                        Result result = results.get(stationNameHash);
                        if (result == null) {
                            result = new Result(stationNameStartAddress, stationNameNumBytes, m, m, m, 1);
                            results.put(stationNameHash, result);
                        } else {
                            if (m < result.min) {
                                result.min = m;
                            }
                            if (m > result.max) {
                                result.max = m;
                            }
                            result.sum = result.sum + m;
                            result.measurements = result.measurements + 1;
                        }
                    }
                    return results;
                });
                futures.add(future);
            }
            List<FinalResult> totalResults = processResults(futures);;
            printResults(totalResults);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<FinalResult> processResults(List<Future<Map<Integer, Result>>> futures) {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        return futures.stream()
                      .map(f -> {
                          try {
                              return f.get();
                          } catch (Exception e) {
                              throw new RuntimeException(e);
                          }
                      })
                      .flatMap(m -> m.entrySet().stream())
                      .collect(Collectors.groupingBy(Map.Entry::getKey,
                              mapping(Map.Entry::getValue, reducing((r1, r2) -> {
                                  r1.min = Math.min(r1.min, r2.min);
                                  r1.max = Math.max(r1.max, r2.max);
                                  r1.sum = r1.sum + r2.sum;
                                  r1.measurements = r1.measurements + r2.measurements;
                                  return r1;
                              }))))
                      .values()
                      .stream()
                      .flatMap(Optional::stream)
                      .map(r -> new FinalResult(
                              getStationName(buffer, r.stationNameStartAddress, r.stationNameNumBytes), r.min,
                              r.max, r.sum, r.measurements))
                      .toList();
    }

    private static String getStationName(ByteBuffer buffer, long stationNameStartAddress, int stationNameNumBytes) {
        buffer.clear();
        for (int i = 0; i < stationNameNumBytes; i++) {
            byte b = UNSAFE.getByte(stationNameStartAddress + i);
            buffer.put(b);
        }
        return new String(buffer.array(), 0, stationNameNumBytes, StandardCharsets.UTF_8);
    }

    private static void printResults(Collection<FinalResult> results) {
        results.stream()
               .sorted(Comparator.comparing(FinalResult::stationName))
               .forEach((e) -> IO.println(MessageFormat.format("{0};{1};{2};{3}", e.stationName(), e.min() / 10d,
                       e.sum() / e.measurements() / 10d, e.max() / 10d)));
    }

    record FinalResult(String stationName, int min, int max, int sum, int measurements) {
    }

    static final class Result {
        public final long stationNameStartAddress;
        public final int stationNameNumBytes;
        public int min;
        public int max;
        public int sum;
        public int measurements;

        Result(long stationNameStartAddress, int stationNameNumBytes, int min, int max, int sum, int measurements) {
            this.stationNameStartAddress = stationNameStartAddress;
            this.stationNameNumBytes = stationNameNumBytes;
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.measurements = measurements;
        }
    }

}