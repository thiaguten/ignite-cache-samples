package br.com.thiaguten;

import static br.com.thiaguten.Environment.CITY_CACHE_NAME;
import static br.com.thiaguten.Environment.PERSON_CACHE_NAME;
import static br.com.thiaguten.Environment.createJdbcTablesAndIndexes;
import static br.com.thiaguten.Environment.keyValueQueryingCityCache;
import static br.com.thiaguten.Environment.keyValueQueryingPersonCache;
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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class IgniteModelCacheDDLwithJDBC {

  public static void main(String[] args) throws Exception {
    Ignite ignite = Environment.newIgnite();

    // Create tables and indexes with JDBC Statement
    createJdbcTablesAndIndexes();

    IgniteCache<Long, City> cityCache = ignite.cache(CITY_CACHE_NAME);
    IgniteCache<PersonPK, Person> personCache = ignite.cache(PERSON_CACHE_NAME);

    System.out.println("> Ignite cache names: " + ignite.cacheNames());

    // 1 - SQL API usage to interact with the cache
    // --------------------------------------------

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

    // 2 - Key-Value API usage to interact with the cache
    // --------------------------------------------------

    City city = new City(4L, "Brasília");
    cityCache.put(city.getId(), city);

    PersonPK personPK = new PersonPK(5L, city.getId());
    Person person = new Person("Thiago");
    personCache.put(personPK, person);

    System.out.println("> [City] total size: " + cityCache.size(CachePeekMode.PRIMARY));
    System.out.println("> [Person] total size: " + personCache.size());

    // Querying caches:
    // ---------------

    sqlDistributedJoinQueryCache(personCache);

    sqlQueryingCityCache(cityCache);

    Set<Long> cityKeys = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L));
    keyValueQueryingCityCache(cityKeys, cityCache);

    sqlQueryingPersonCache(personCache);

    Set<PersonPK> personKeys = new HashSet<>(Arrays.asList(
        new PersonPK(1L, 3L),
        new PersonPK(2L, 2L),
        new PersonPK(3L, 1L),
        new PersonPK(4L, 2L),
        new PersonPK(5L, 4L)));
    keyValueQueryingPersonCache(personKeys, personCache);
  }

}
