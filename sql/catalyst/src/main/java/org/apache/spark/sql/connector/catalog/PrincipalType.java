package org.apache.spark.sql.connector.catalog;

/**
 * An enumeration support for Role-Based Access Control(RBAC) extensions.
 */
public enum PrincipalType {
  USER,
  GROUP,
  ROLE
}
