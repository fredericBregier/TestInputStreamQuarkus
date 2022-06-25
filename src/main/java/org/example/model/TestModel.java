package org.example.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class TestModel {
  public String var1;
  public String var2;

  public TestModel() {
  }

  public TestModel(final String var1, final String var2) {
    this.var1 = var1;
    this.var2 = var2;
  }

}
