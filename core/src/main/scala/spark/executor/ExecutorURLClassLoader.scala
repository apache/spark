package spark.executor

import java.net.{URLClassLoader, URL}

/**
 * The addURL method in URLClassLoader is protected. We subclass it to make this accessible.
 */
private[spark] class ExecutorURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends URLClassLoader(urls, parent) {

  override def addURL(url: URL) {
    super.addURL(url)
  }
}
