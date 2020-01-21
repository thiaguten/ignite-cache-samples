package br.com.thiaguten.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Person {

  @QuerySqlField(index = true)
  private String NAME;

  public Person(String name) {
    this.NAME = name;
  }

  public String getName() {
    return NAME;
  }

  public void setName(String name) {
    this.NAME = name;
  }

}
