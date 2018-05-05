package org.apache.spark.sql.sources.v2;

import org.apache.spark.sql.sources.v2.catalog.DataSourceCatalog;

/**
 * A mix-in interface for {@link DataSourceV2} catalog support. Data sources can implement this
 * interface to provide the ability to load, create, alter, and drop tables.
 * <p>
 * Data sources must implement this interface to support logical operations that combine writing
 * data with catalog tasks, like create-table-as-select.
 */
public interface CatalogSupport {
  /**
   * Return a {@link DataSourceCatalog catalog} for tables.
   *
   * @return a catalog
   */
  DataSourceCatalog catalog();
}
