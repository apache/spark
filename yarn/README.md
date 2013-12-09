# YARN DIRECTORY LAYOUT

Hadoop Yarn related codes are organized in separate modules with layered dependency.

 * common : Common codes that will be called into by other modules.

 * 2.0 / 2.2 : Codes that involve specific version of Hadoop YARN API. Depends on common module.

  2.0 actually represents  0.23 and 2.0
  2.2 actually represents 2.2 and later, until the API is break again.

 * Scheduler : Implementation of various YARN Scheduler and backend. Depends on 2.0 / 2.2 modules.
 * Assembly : For maven build to assembly all other modules in Yarn into one single jar.

