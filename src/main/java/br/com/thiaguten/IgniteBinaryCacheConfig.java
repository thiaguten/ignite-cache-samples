package br.com.thiaguten;

import static br.com.thiaguten.Environment.CITY_CACHE_NAME;
import static br.com.thiaguten.Environment.CITY_VALUE_TYPE;
import static br.com.thiaguten.Environment.PERSON_CACHE_NAME;
import static br.com.thiaguten.Environment.PERSON_KEY_TYPE;
import static br.com.thiaguten.Environment.PERSON_VALUE_TYPE;
import static br.com.thiaguten.Environment.SCHEMA;

import br.com.thiaguten.model.City;
import br.com.thiaguten.model.Person;
import br.com.thiaguten.model.PersonPK;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

public class IgniteBinaryCacheConfig {

  static Ignite ignite = Environment.newIgnite();
  static IgniteBinary binary = ignite.binary();

  public static void main(String[] args) {
    CacheConfiguration<Long, City> cityCacheConfig = new CacheConfiguration<>();
    cityCacheConfig.setName(CITY_CACHE_NAME);
    cityCacheConfig.setSqlSchema(SCHEMA);
    cityCacheConfig.setCacheMode(CacheMode.REPLICATED);
    cityCacheConfig.setIndexedTypes(Long.class, City.class);

    CacheConfiguration<PersonPK, Person> personCacheConfig = new CacheConfiguration<>();
    personCacheConfig.setName(PERSON_CACHE_NAME);
    personCacheConfig.setSqlSchema(SCHEMA);
    personCacheConfig.setBackups(1);
    personCacheConfig.setCacheMode(CacheMode.PARTITIONED);
    personCacheConfig.setIndexedTypes(PersonPK.class, Person.class);

    IgniteCache<Long, BinaryObject> cityCache = ignite.getOrCreateCache(cityCacheConfig).withKeepBinary();
    IgniteCache<BinaryObject, BinaryObject> personCache = ignite.getOrCreateCache(personCacheConfig).withKeepBinary();

    System.out.println("> Ignite cache names: " + ignite.cacheNames());

    // 1 - SQL API:
    // ------------
    SqlFieldsQuery insertCity = new SqlFieldsQuery(
        "INSERT INTO City (_key, id, name) VALUES (?, ?, ?)");
    cityCache.query(insertCity.setArgs(1L, 1L, "Forest Hill")).getAll();
    cityCache.query(insertCity.setArgs(2L, 2L, "Denver")).getAll();
    cityCache.query(insertCity.setArgs(3L, 3L, "St. Petersburg")).getAll();

    SqlFieldsQuery insertPerson = new SqlFieldsQuery(
        "INSERT INTO Person (id, city_id, name) VALUES (?, ?, ?)");
    personCache.query(insertPerson.setArgs(1L, 3L, "John Doe")).getAll();
    personCache.query(insertPerson.setArgs(2L, 2L, "Jane Roe")).getAll();
    personCache.query(insertPerson.setArgs(3L, 1L, "Mary Major")).getAll();
    personCache.query(insertPerson.setArgs(4L, 2L, "Richard Miles")).getAll();

    // 2 - Key-Value API:
    // --------------------------

    //    2.1 - using model to binary
    City city = new City(4L, "Brasília");
    cityCache.put(city.getId(), binary.toBinary(city));

    PersonPK personPK = new PersonPK(5L, city.getId());
    Person person = new Person("Thiago");
    personCache.put(binary.toBinary(personPK), binary.toBinary(person));

    //    2.2 - using binary builder
    BinaryObject cityVal = binary.builder(CITY_VALUE_TYPE)
        .setField("id", 5L)
        .setField("name", "Natalândia")
        .build();
    cityCache.put(cityVal.field("id"), cityVal);

    BinaryObject personKey = binary.builder(PERSON_KEY_TYPE)
        .setField("id", 6L)
        .setField("city_id", 5L)
        .build();

    BinaryObject personVal = binary.builder(PERSON_VALUE_TYPE)
        .setField("name", "Dayana")
        .build();

    personCache.put(personKey, personVal);

    System.out.println("> [City] total size: " + cityCache.size(CachePeekMode.PRIMARY));
    System.out.println("> [Person] total size: " + personCache.size());

    // Querying caches:
    // ---------------
    sqlQueryingCityCache(cityCache);
    keyValueQueryingCityCache(cityCache);

    sqlQueryingPersonCache(personCache);
    keyValueQueryingPersonCache(personCache);

    sqlDistributedJoinQueryCache(personCache);
  }

  private static void sqlQueryingCityCache(IgniteCache<?, ?> cache) {
    System.out.println("> [City] SQL query result:");
    SqlFieldsQuery select = new SqlFieldsQuery("SELECT * FROM City ORDER BY id");
    try (FieldsQueryCursor<List<?>> cursor = cache.query(select)) {
      for (List<?> row : cursor) {
        String rowValues = row.stream().map(Object::toString).collect(Collectors.joining(", "));
        System.out.println("\t" + rowValues);
      }
    }
  }

  private static void sqlQueryingPersonCache(IgniteCache<?, ?> cache) {
    System.out.println("> [Person] SQL query result:");
    SqlFieldsQuery select = new SqlFieldsQuery("SELECT * FROM Person ORDER BY id");
    try (FieldsQueryCursor<List<?>> cursor = cache.query(select)) {
      for (List<?> row : cursor) {
        String rowValues = row.stream().map(Object::toString).collect(Collectors.joining(", "));
        System.out.println("\t" + rowValues);
      }
    }
  }

  private static void keyValueQueryingCityCache(IgniteCache<Long, BinaryObject> cityCache) {
    System.out.println("> [City] Key-Value query result:");
    Set<Long> keys = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L, 5L));

//    keys.forEach(key -> System.out.println((String) cityCache.get(key).field("name")));

//    Map<Long, BinaryObject> map = cityCache.getAll(keys);

////    TreeMap<Long, BinaryObject> sortedMap = new TreeMap<>(Comparator.comparing(Function.identity()));
////    sortedMap.putAll(map);
//    TreeMap<Long, BinaryObject> sortedMap = new TreeMap<>(map);
//    sortedMap.forEach((id,binaryObject) ->
//        System.out.println("\t" + id + ", " + binaryObject.field("name")));

//    map.entrySet()
//        .stream()
//        .sorted(Map.Entry.comparingByKey())
//        .map(Map.Entry::getValue)
//        .map(binaryObject -> "\t" + binaryObject.field("id") + ", " + binaryObject.field("name"))
//        .forEach(System.out::println);

    Collection<CacheEntry<Long, BinaryObject>> entries = cityCache.getEntries(keys);
    entries.stream()
        .sorted(Comparator.comparing(CacheEntry::getKey))
        .map(CacheEntry::getValue)
        .map(binaryObject -> "\t" + binaryObject.field("id") + ", " + binaryObject.field("name"))
        .forEach(System.out::println);
  }

  private static void keyValueQueryingPersonCache(IgniteCache<BinaryObject, BinaryObject> personCache) {
    System.out.println("> [Person] Key-Value query result:");
    BinaryObject person1Key = binary.builder(PERSON_KEY_TYPE)
        .setField("id", 1L)
        .setField("city_id", 3L)
        .build();
    BinaryObject person2Key = binary.builder(PERSON_KEY_TYPE)
        .setField("id", 2L)
        .setField("city_id", 2L)
        .build();
    BinaryObject person3Key = binary.builder(PERSON_KEY_TYPE)
        .setField("id", 3L)
        .setField("city_id", 1L)
        .build();
    BinaryObject person4Key = binary.builder(PERSON_KEY_TYPE)
        .setField("id", 4L)
        .setField("city_id", 2L)
        .build();
    BinaryObject person5Key = binary.builder(PERSON_KEY_TYPE)
        .setField("id", 5L)
        .setField("city_id", 4L)
        .build();
    BinaryObject person6Key = binary.builder(PERSON_KEY_TYPE)
        .setField("id", 6L)
        .setField("city_id", 5L)
        .build();

    Set<BinaryObject> keys = new HashSet<>(
        Arrays.asList(person1Key, person2Key, person3Key, person4Key, person5Key, person6Key));

//    keys.forEach(key -> System.out.println((String) personCache.get(key).field("name")));

//    Map<BinaryObject, BinaryObject> map = personCache.getAll(keys);

//    TreeMap<BinaryObject, BinaryObject> sortedMap = new TreeMap<>(Comparator.comparing(binary -> binary.field("id")));
//    sortedMap.putAll(map);
//    sortedMap.forEach((k,v) ->
//        System.out.println("\t" + k.field("id") + ", " + k.field("city_id") + ", " + v.field("name")));

//    map.entrySet()
//        .stream()
//        .sorted(Comparator.comparing(cacheEntry -> cacheEntry.getKey().field("id")))
//        .map(entry -> {
//          BinaryObject key = entry.getKey();
//          BinaryObject value = entry.getValue();
//          return "\t" + key.field("id") + ", " + key.field("city_id") + ", " + value.field("name");
//        })
//        .forEach(System.out::println);

    Collection<CacheEntry<BinaryObject, BinaryObject>> entries = personCache.getEntries(keys);
    entries.stream()
        .sorted(Comparator.comparing(cacheEntry -> cacheEntry.getKey().field("id")))
        .map(entry -> {
          BinaryObject key = entry.getKey();
          BinaryObject value = entry.getValue();
          return "\t" + key.field("id") + ", " + key.field("city_id") + ", " + value.field("name");
        })
        .forEach(System.out::println);
  }

  private static void sqlDistributedJoinQueryCache(IgniteCache<?, ?> cache) {
    String cacheName = cache.getName();

    // Querying data from the cluster using a distributed JOIN.
    SqlFieldsQuery select = new SqlFieldsQuery(
        "SELECT p.name, c.name FROM Person p, City c WHERE p.city_id = c.id");

    System.out.println("> ["+cacheName+"] SQL query result using a distributed JOIN:");
    try (FieldsQueryCursor<List<?>> cursor = cache.query(select)) {
      for (List<?> row : cursor) {
        String rowValues = row.stream().map(Object::toString).collect(Collectors.joining(", "));
        System.out.println("\t" + rowValues);
      }
    }
  }

}
