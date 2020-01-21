package br.com.thiaguten;

import static br.com.thiaguten.Environment.CITY_CACHE_NAME;
import static br.com.thiaguten.Environment.CITY_VALUE_TYPE;
import static br.com.thiaguten.Environment.PERSON_CACHE_NAME;
import static br.com.thiaguten.Environment.PERSON_KEY_TYPE;
import static br.com.thiaguten.Environment.PERSON_VALUE_TYPE;
import static br.com.thiaguten.Environment.SCHEMA;
import static br.com.thiaguten.Environment.keyValueQueryingCityCacheBinary;
import static br.com.thiaguten.Environment.keyValueQueryingPersonCacheBinary;
import static br.com.thiaguten.Environment.sqlDistributedJoinQueryCache;
import static br.com.thiaguten.Environment.sqlQueryingCityCache;
import static br.com.thiaguten.Environment.sqlQueryingPersonCache;

import br.com.thiaguten.model.City;
import br.com.thiaguten.model.Person;
import br.com.thiaguten.model.PersonPK;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

public class IgniteBinaryCacheConfig {

  public static void main(String[] args) {
    Ignite ignite = Environment.newIgnite();
    IgniteBinary binary = ignite.binary();

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
    sqlDistributedJoinQueryCache(personCache);

    sqlQueryingCityCache(cityCache);

    Set<Long> cityKeys = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L, 5L));
    keyValueQueryingCityCacheBinary(cityKeys, cityCache);

    sqlQueryingPersonCache(personCache);

    Set<BinaryObject> personKeys = new HashSet<>(Arrays.asList(
        binary.builder(PERSON_KEY_TYPE) // person1Key
            .setField("id", 1L)
            .setField("city_id", 3L)
            .build(),
        binary.builder(PERSON_KEY_TYPE) // person2Key
            .setField("id", 2L)
            .setField("city_id", 2L)
            .build(),
        binary.builder(PERSON_KEY_TYPE) // person3Key
            .setField("id", 3L)
            .setField("city_id", 1L)
            .build(),
        binary.builder(PERSON_KEY_TYPE) // person4Key
            .setField("id", 4L)
            .setField("city_id", 2L)
            .build(),
        binary.builder(PERSON_KEY_TYPE) // person5Key
            .setField("id", 5L)
            .setField("city_id", 4L)
            .build(),
        binary.builder(PERSON_KEY_TYPE) // person6Key
            .setField("id", 6L)
            .setField("city_id", 5L)
            .build()));
    keyValueQueryingPersonCacheBinary(personKeys, personCache);
  }

}
