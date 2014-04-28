package bench;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Main {

  private static final int DATA_SIZE = 100;
  private static final long COUNT = 1_000_000;
  private static final long RANDOM_UPDATE_COUNT = 100_000;
  private static List<Integer> toBeUpdate;
  private static String bigData;

  public static void main(String[] args) {
    generateData();

    generateIdToUpdate();

    System.out.println("------- PERSISIT 1 -----------");
    test(new PersisitItCache<String, Measure>());

    System.out.println("------- LUCENE 1 -----------");
    test(new LuceneCache<>(Measure.class));

    // System.out.println("------- MAPDB 1 -----------");
    // test(new MapDBCache<Measure>());

    System.out.println("------- PERSISIT 2 -----------");
    test(new PersisitItCache<String, Measure>());

    System.out.println("------- LUCENE 2 -----------");
    test(new LuceneCache<>(Measure.class));

    // System.out.println("------- MAPDB 2 -----------");
    // test(new MapDBCache<Measure>());
  }

  private static void generateIdToUpdate() {
    toBeUpdate = new ArrayList<>();
    for (int i = 0; i < RANDOM_UPDATE_COUNT; i++) {
      toBeUpdate.add((int) (Math.random() * COUNT));
    }
  }

  private static void generateData() {
    StringBuilder data = new StringBuilder();
    for (long i = 0; i < DATA_SIZE; i++) {
      data.append("foo \n");
    }
    bigData = data.toString();
  }

  private static void test(Cache<String, Measure> cache) {
    long start = System.currentTimeMillis();
    for (long i = 0; i < COUNT; i++) {
      cache.put("key" + i, new Measure("key" + i, bigData, i, new Date()));
    }
    System.out.println("Insert: " + (System.currentTimeMillis() - start) + "ms");

    start = System.currentTimeMillis();
    long count = 0;
    for (Measure m : cache.values()) {
      count++;
      if (!m.getData().equals(bigData)) {
        throw new RuntimeException();
      }
    }
    System.out.println("Iterate all " + count + " elements: " + (System.currentTimeMillis() - start) + "ms");

    start = System.currentTimeMillis();
    for (long i = 0; i < COUNT; i++) {
      Measure m = cache.get("key" + i);
      if (!m.getData().equals(bigData) || !m.getKey().equals("key" + i)) {
        throw new RuntimeException();
      }
    }
    System.out.println("Access all by key: " + (System.currentTimeMillis() - start) + "ms");

    start = System.currentTimeMillis();
    for (int i = 0; i < RANDOM_UPDATE_COUNT; i++) {
      Integer id = toBeUpdate.get(i);
      Measure m = cache.get("key" + id);
      if (!m.getData().equals(bigData) || !m.getKey().equals("key" + id)) {
        throw new RuntimeException();
      }
      m.setValue(2L * id);
      cache.put("key" + id, m);
      m = cache.get("key" + id);
      if (!m.getData().equals(bigData) || !m.getKey().equals("key" + id) || !m.getValue().equals(2L * id)) {
        throw new RuntimeException();
      }
    }
    System.out.println("Random update: " + (System.currentTimeMillis() - start) + "ms");

    cache.close();
    System.gc();
  }

  private static class Measure implements Serializable {
    private String key;
    private String data;
    private Long value;
    private Date date;

    public Measure(String key, String data, Long value, Date date) {
      this.key = key;
      this.data = data;
      this.value = value;
      this.date = date;
    }

    public String getData() {
      return data;
    }

    public String getKey() {
      return key;
    }

    public void setData(String data) {
      this.data = data;
    }

    public void setValue(Long value) {
      this.value = value;
    }

    public Long getValue() {
      return value;
    }
  }

}
