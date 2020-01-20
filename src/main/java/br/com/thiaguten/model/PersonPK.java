package br.com.thiaguten.model;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class PersonPK {

  @QuerySqlField
  private Long id;

  @QuerySqlField(name = "city_id")
  @AffinityKeyMapped
  private Long cityId;

  public PersonPK(Long id, Long cityId) {
    this.id = id;
    this.cityId = cityId;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getCityId() {
    return cityId;
  }

  public void setCityId(Long cityId) {
    this.cityId = cityId;
  }

}
