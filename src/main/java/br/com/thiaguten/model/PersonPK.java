package br.com.thiaguten.model;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class PersonPK {

  @QuerySqlField
  private Long ID;

  @QuerySqlField
  @AffinityKeyMapped
  private Long CITY_ID;

  public PersonPK(Long id, Long cityId) {
    this.ID = id;
    this.CITY_ID = cityId;
  }

  public Long getId() {
    return ID;
  }

  public void setId(Long id) {
    this.ID = id;
  }

  public Long getCityId() {
    return CITY_ID;
  }

  public void setCityId(Long cityId) {
    this.CITY_ID = cityId;
  }

}
