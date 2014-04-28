package bench;

import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

public class Main {

  private static final long COUNT = 1_000_000;

  public static void main(String[] args) throws ClassNotFoundException, IOException, ParseException {
    StringBuilder bigData = new StringBuilder();
    for (long i = 0; i < 100; i++) {
      bigData.append("foo \n");
    }

    System.out.println("------- PERSISIT 1 -----------");
    test(new PersisitItCache<String, Measure>(), bigData.toString());

    System.out.println("------- LUCENE 1 -----------");
    test(new LuceneCache<>(Measure.class), bigData.toString());

    // System.out.println("------- MAPDB 1 -----------");
    // test(new MapDBCache<Measure>(), bigData.toString());

    System.out.println("------- PERSISIT 2 -----------");
    test(new PersisitItCache<String, Measure>(), bigData.toString());

    System.out.println("------- LUCENE 2 -----------");
    test(new LuceneCache<>(Measure.class), bigData.toString());

    // System.out.println("------- MAPDB 2 -----------");
    // test(new MapDBCache<Measure>(), bigData.toString());
  }

  private static void test(Cache<String, Measure> cache, String data) throws IOException, ParseException, ClassNotFoundException {
    long start = System.currentTimeMillis();
    for (long i = 0; i < COUNT; i++) {
      cache.put("key" + i, new Measure("key" + i, data, i, new Date()));
    }
    System.out.println("Insert: " + (System.currentTimeMillis() - start) + "ms");

    start = System.currentTimeMillis();
    long count = 0;
    for (Measure m : cache.values()) {
      count++;
      if (!m.getData().equals(data)) {
        throw new RuntimeException();
      }
    }
    System.out.println("Iterate all " + count + " elements: " + (System.currentTimeMillis() - start) + "ms");

    start = System.currentTimeMillis();
    for (long i = 0; i < COUNT; i++) {
      Measure m = cache.get("key" + i);
      if (!m.getData().equals(data) || !m.getKey().equals("key" + i)) {
        throw new RuntimeException();
      }
    }
    System.out.println("Access all by key: " + (System.currentTimeMillis() - start) + "ms");

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
  }

}
