package org.apache.spark.sql.connector.expressions;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.filter.Filter;

import java.io.Serializable;

/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * When a = true, returns b; when c = true, returns d; else returns e.
 *
 * @since 3.3.0
 */
@Evolving
public final class CaseWhen implements Expression, Serializable {
  private final Filter[] conditions;
  private final Literal[] values;
  private final Literal elseValue;

  public CaseWhen(Filter[] conditions, Literal[] values, Literal elseValue) {
    this.conditions = conditions;
    this.values = values;
    this.elseValue = elseValue;
  }

  public Filter[] conditions() { return conditions; }
  public Literal[] values() { return values; }
  public Literal elseValue() { return elseValue; }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CASE");
    for (int i = 0; i < conditions.length; i++) {
      sb.append(" WHEN ");
      sb.append(conditions[i]);
      sb.append(" THEN ");
      sb.append(values[i]);
    }
    if (elseValue != null) {
      sb.append(" ELSE ");
      sb.append(elseValue);
    }
    sb.append(" END");
    return sb.toString();
  }
}
