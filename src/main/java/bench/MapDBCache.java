package bench;

/*
 * SonarQube, open source software quality management tool.
 * Copyright (C) 2008-2014 SonarSource
 * mailto:contact AT sonarsource DOT com
 *
 * SonarQube is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * SonarQube is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

import org.apache.commons.io.FileUtils;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Factory of caches
 *
 * @since 3.6
 */
public class MapDBCache<V extends Serializable> implements Cache<String, V> {

  private File tempFile;
  private DB db;
  private ConcurrentNavigableMap<String, V> cache;

  public MapDBCache() {
    initMapDB();
  }

  private void initMapDB() {
    try {
      tempFile = new File("target/mapdb.cache");
      db = DBMaker.newFileDB(tempFile)
        .cacheSoftRefEnable()
        .mmapFileEnableIfSupported()
        .transactionDisable()
        .make();
      this.cache = createStringCache("sample");
    } catch (Exception e) {
      throw new IllegalStateException("Fail to start caches", e);
    }
  }

  @Override
  public void close() {
    if (db != null) {
      db.close();
      db = null;
    }
    FileUtils.deleteQuietly(tempFile);
    tempFile = null;
  }

  @Override
  public Cache<String, V> put(String key, V value) {
    cache.put(key, value);
    return this;
  }

  @Override
  public V get(String key) {
    return cache.get(key);
  }

  @Override
  public Iterable<V> values() {
    return cache.values();
  }

  @Override
  public boolean containsKey(String key) {
    return cache.containsKey(key);
  }

  public <V extends Serializable> ConcurrentNavigableMap<String, V> createStringCache(String cacheName) {
    return createCache(cacheName, BTreeKeySerializer.STRING);
  }

  public <A, B, V extends Serializable> ConcurrentNavigableMap<Fun.Tuple2<A, B>, V> createTuple2Cache(String cacheName) {
    return createCache(cacheName, BTreeKeySerializer.TUPLE2);
  }

  public <A, B, C, V extends Serializable> ConcurrentNavigableMap<Fun.Tuple3<A, B, C>, V> createTuple3Cache(String cacheName) {
    return createCache(cacheName, BTreeKeySerializer.TUPLE3);
  }

  private <K, V extends Serializable> ConcurrentNavigableMap<K, V> createCache(String cacheName, BTreeKeySerializer<K> keySerializer) {
    try {
      ConcurrentNavigableMap<K, V> map = db
        .createTreeMap(cacheName)
        .keySerializer(keySerializer)
        .valueSerializer(new KryoSerializer())
        // .valuesOutsideNodesEnable()
        .make();
      return map;
    } catch (Exception e) {
      throw new IllegalStateException("Fail to create cache: " + cacheName, e);
    }
  }

  File tempFile() {
    return tempFile;
  }
}
