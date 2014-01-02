# YARN DIRECTORY LAYOUT

Hadoop Yarn related codes are organized in separate directories for easy management.

 * common : Common codes that do not depending on specific version of Hadoop.

 * alpha / stable : Codes that involve specific version of Hadoop YARN API.

  alpha represents 0.23 and 2.0.x
  stable represents 2.2 and later, until the API is break again.

alpha / stable will build together with common dir into a single jar
