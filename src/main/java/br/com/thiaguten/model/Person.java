package br.com.thiaguten.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Person {

  @QuerySqlField(index = true)
  private String name;

  public Person(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

}
