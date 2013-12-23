# YARN DIRECTORY LAYOUT

Hadoop Yarn related codes are organized in separate directories for easy management.

 * common : Common codes that do not depending on specific version of Hadoop.

 * 2.0 / 2.2 : Codes that involve specific version of Hadoop YARN API.

  2.0 actually represents  0.23 and 2.0
  2.2 actually represents 2.2 and later, until the API is break again.

2.0 / 2.2 will build together with common dir into a single jar
