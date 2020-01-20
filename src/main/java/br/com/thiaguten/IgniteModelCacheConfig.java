package br.com.thiaguten;

import static br.com.thiaguten.Environment.CITY_CACHE_NAME;
import static br.com.thiaguten.Environment.PERSON_CACHE_NAME;
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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

public class IgniteModelCacheConfig {

  public static void main(String[] args) {
    Ignite ignite = Environment.newIgnite();

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

    IgniteCache<Long, City> cityCache = ignite.getOrCreateCache(cityCacheConfig);
    IgniteCache<PersonPK, Person> personCache = ignite.getOrCreateCache(personCacheConfig);

    System.out.println("> Ignite cache names: " + ignite.cacheNames());

    // 1 - SQL API:
    // ------------
    SqlFieldsQuery insertCity = new SqlFieldsQuery(
        "INSERT INTO City (_key, id, name) VALUES (?, ?, ?)");
    cityCache.query(insertCity.setArgs(1L, 1L, "Forest Hill")).getAll();
    cityCache.query(insertCity.setArgs(2L, 2L, "Denver")).getAll();
    cityCache.query(insertCity.setArgs(3L, 3L, "St. Petersburg")).getAll();

    SqlFieldsQuery insertPerson = new SqlFieldsQuery(
        "INSERT INTO Person (id, name, city_id) VALUES (?, ?, ?)");
    personCache.query(insertPerson.setArgs(1L, "John Doe", 3L)).getAll();
    personCache.query(insertPerson.setArgs(2L, "Jane Roe", 2L)).getAll();
    personCache.query(insertPerson.setArgs(3L, "Mary Major", 1L)).getAll();
    personCache.query(insertPerson.setArgs(4L, "Richard Miles", 2L)).getAll();

    // 2 - Key-Value API:
    // --------------------------
    City city = new City(4L, "Brasília");
    cityCache.put(city.getId(), city);

    PersonPK personPK = new PersonPK(5L, city.getId());
    Person person = new Person("Thiago");
    personCache.put(personPK, person);

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

  private static void keyValueQueryingCityCache(IgniteCache<Long, City> cityCache) {
    System.out.println("> [City] Key-Value query result:");
    Set<Long> keys = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L, 5L));

//    keys.forEach(key -> System.out.println(cityCache.get(key).getName()));

//    Map<Long, City> map = cityCache.getAll(keys);

////    TreeMap<Long, City> sortedMap = new TreeMap<>(Comparator.comparing(Function.identity()));
////    sortedMap.putAll(map);
//    TreeMap<Long, City> sortedMap = new TreeMap<>(map);
//    sortedMap.forEach((id,city) -> System.out.println("\t" + id + ", " + city.getName()));

//    map.entrySet()
//        .stream()
//        .sorted(Map.Entry.comparingByKey())
//        .map(Map.Entry::getValue)
//        .map(city -> "\t" + city.getId() + ", " + city.getName())
//        .forEach(System.out::println);

    Collection<CacheEntry<Long, City>> entries = cityCache.getEntries(keys);
    entries.stream()
        .sorted(Comparator.comparing(CacheEntry::getKey))
        .map(CacheEntry::getValue)
        .map(city -> "\t" + city.getId() + ", " + city.getName())
        .forEach(System.out::println);
  }

  private static void keyValueQueryingPersonCache(IgniteCache<PersonPK, Person> personCache) {
    System.out.println("> [Person] Key-Value query result:");
    PersonPK person1Key = new PersonPK(1L, 3L);
    PersonPK person2Key = new PersonPK(2L, 2L);
    PersonPK person3Key = new PersonPK(3L, 1L);
    PersonPK person4Key = new PersonPK(4L, 2L);
    PersonPK person5Key = new PersonPK(5L, 4L);
    PersonPK person6Key = new PersonPK(6L, 5L);

    Set<PersonPK> keys = new HashSet<>(
        Arrays.asList(person1Key, person2Key, person3Key, person4Key, person5Key, person6Key));

//    keys.forEach(key -> System.out.println(personCache.get(key).getName()));

//    Map<PersonPK, Person> map = personCache.getAll(keys);

//    TreeMap<PersonPK, Person> sortedMap = new TreeMap<>(Comparator.comparing(PersonPK::getId));
//    sortedMap.putAll(map);
//    sortedMap.forEach((personPK,person) ->
//        System.out.println("\t" + personPK.getId() + ", " + personPK.getCityId() + ", " + person.getName()));

//    map.entrySet()
//        .stream()
//        .sorted(Comparator.comparing(cacheEntry -> cacheEntry.getKey().getId()))
//        .map(entry -> {
//          PersonPK key = entry.getKey();
//          Person value = entry.getValue();
//          return "\t" + key.getId() + ", " + key.getCityId() + ", " + value.getName();
//        })
//        .forEach(System.out::println);

    Collection<CacheEntry<PersonPK, Person>> entries = personCache.getEntries(keys);
    entries.stream()
        .sorted(Comparator.comparing(cacheEntry -> cacheEntry.getKey().getId()))
        .map(entry -> {
          PersonPK key = entry.getKey();
          Person value = entry.getValue();
          return "\t" + key.getId() + ", " + key.getCityId() + ", " + value.getName();
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
