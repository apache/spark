---
layout: global
title: Geospatial (Geometry/Geography) Types
displayTitle: Geospatial (Geometry/Geography) Types
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Spark SQL supports **GEOMETRY** and **GEOGRAPHY** types for spatial data, as defined in the [Open Geospatial Consortium (OGC) Simple Feature Access](https://portal.ogc.org/files/?artifact_id=25355) specification. At runtime, values are represented as **Well-Known Binary (WKB)** and are associated with a **Spatial Reference Identifier (SRID)** that defines the coordinate system. How values are persisted is determined by each data source.

### Overview

| Type | Coordinate system | Typical use and notes |
|------|-------------------|------------------------|
| **GEOMETRY** | Cartesian (planar) | Projected or local coordinates; planar calculations. Represents points, lines, polygons in a flat coordinate system. Suitable for Web Mercator (SRID 3857), UTM, or local grids (e.g. engineering/CAD). Default SRID in Spark is 4326. |
| **GEOGRAPHY** | Geographic (latitude/longitude) | Earth-based data; distances and areas on the sphere/ellipsoid. Coordinates in longitude and latitude (degrees). Edge interpolation is always **SPHERICAL**. Default SRID is 4326 (WGS 84). |

#### When to use GEOMETRY vs GEOGRAPHY

Choose **GEOMETRY** when:

* Data is in **local or projected coordinates** (e.g. engineering/CAD in meters, or map tiles in Web Mercator).
* You need **planar operations** on a small or regional area: intersections, unions, clipping, containment, or overlays where treating the surface as flat is acceptable.
* Vertices are closely spaced or the extent is small enough that Earth curvature is negligible.

Choose **GEOGRAPHY** when:

* Data is **global** or spans large extents (e.g. country boundaries, worldwide points of interest).
* **Distances or areas** must respect Earth curvature (e.g. the shortest path between two cities, or the area of a polygon on the globe).
* Use cases include **aviation, maritime, or global mobility** where great-circle or geodesic behavior matters.

Using the wrong type can give misleading results: for example, the shortest path between London and New York on a sphere crosses Canada, whereas a planar GEOMETRY may suggest a path that does not.

### Type Syntax in SQL

In SQL you must specify the type with an SRID or `ANY`:

* **Fixed SRID** (all values in the column share one SRID):
  * `GEOMETRY(srid)` — e.g. `GEOMETRY(4326)`, `GEOMETRY(3857)`
  * `GEOGRAPHY(srid)` — e.g. `GEOGRAPHY(4326)`
* **Mixed SRID** (values in the column may have different SRIDs):
  * `GEOMETRY(ANY)`
  * `GEOGRAPHY(ANY)`

Unparameterized `GEOMETRY` or `GEOGRAPHY` (without `(srid)` or `(ANY)`) is not supported in SQL.

### Creating Tables with Geometry or Geography Columns

```sql
-- Fixed SRID: all values must use the given SRID (e.g. WGS 84)
CREATE TABLE points (
  id BIGINT,
  pt GEOMETRY(4326)
);

CREATE TABLE locations (
  id BIGINT,
  loc GEOGRAPHY(4326)
);

-- Mixed SRID: each row can have a different SRID
CREATE TABLE mixed_geoms (
  id BIGINT,
  geom GEOMETRY(ANY)
);
```

### Constructing Geometry and Geography Values

Values are created from **Well-Known Binary (WKB)** using built-in functions. WKB is a standard binary encoding for spatial shapes (points, lines, polygons, etc.). See [Well-known binary](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary) for the format.

**From WKB (binary):**

* `ST_GeomFromWKB(wkb)` — returns GEOMETRY with default SRID 0.
* `ST_GeomFromWKB(wkb, srid)` — returns GEOMETRY with the given SRID.
* `ST_GeogFromWKB(wkb)` — returns GEOGRAPHY with SRID 4326.

**Example (point in WKB, then use in a table):**

```sql
-- Point (1, 2) in WKB (little-endian point, 2D)
SELECT ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040');
SELECT ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040', 4326);
SELECT ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040');

INSERT INTO points (id, pt)
VALUES (1, ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040', 4326));
```

#### WKB coordinate handling

When parsing WKB, Spark applies the following rules. Violations result in a parse error.

* **Empty points**: For **Point** geometries (including points inside MultiPoint), **NaN** (Not a Number) coordinate values are allowed and represent an **empty point** (e.g. `POINT EMPTY` in Well-Known Text). **LineString** and **Polygon** (and points inside them) do not allow NaN in coordinate values.
* **Non-point coordinates**: Coordinate values in **LineString**, **Polygon** rings, and points that are part of those structures must be **finite** (no NaN, no positive or negative infinity).
* **Infinity**: **Positive or negative infinity** is never accepted in any coordinate value.
* **Polygon rings**: Each ring must be **closed** (first and last point equal) and have **at least 4 points**. A **LineString** must have at least 2 points.
* **GEOGRAPHY bounds**: When WKB is parsed as **GEOGRAPHY** (e.g. via `ST_GeogFromWKB`), longitude must be in **[-180, 180]** (inclusive) and latitude in **[-90, 90]** (inclusive). GEOMETRY does not enforce these bounds.
* **Invalid WKB**: Null or empty input, truncated bytes, invalid geometry class or byte order, or other malformed WKB.

### Built-in Geospatial (ST) Functions

Spark SQL provides scalar functions for working with GEOMETRY and GEOGRAPHY values. They are grouped under **st_funcs** in the [Built-in Functions](sql-ref-functions-builtin.html) API.

| Function | Description |
|----------|-------------|
| `ST_AsBinary(geo)` | Returns the GEOMETRY or GEOGRAPHY value as WKB (BINARY). |
| `ST_GeomFromWKB(wkb)` | Parses WKB and returns a GEOMETRY with default SRID 0. |
| `ST_GeomFromWKB(wkb, srid)` | Parses WKB and returns a GEOMETRY with the given SRID. |
| `ST_GeogFromWKB(wkb)` | Parses WKB and returns a GEOGRAPHY with SRID 4326. |
| `ST_Srid(geo)` | Returns the SRID of the GEOMETRY or GEOGRAPHY value (NULL if input is NULL). |
| `ST_SetSrid(geo, srid)` | Returns a new GEOMETRY or GEOGRAPHY with the given SRID. |

**Examples:**

```sql
SELECT hex(ST_AsBinary(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040')));
-- 0101000000000000000000F03F0000000000000040

SELECT ST_Srid(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040'));
-- 4326

SELECT ST_Srid(ST_SetSrid(ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040'), 3857));
-- 3857
```

### SRID and Stored Values

* **Fixed-SRID columns**: Every value in the column must have the same SRID as the column type. Inserting a value with a different SRID can raise an error (or you can use `ST_SetSrid` to set the value’s SRID to match the column).
* **Mixed-SRID columns** (`GEOMETRY(ANY)` or `GEOGRAPHY(ANY)`): Values can have different SRIDs. Only valid SRIDs are allowed.
* **Storage**: Parquet, Delta, and Iceberg store geometry/geography with a fixed SRID per column; mixed-SRID types are for in-memory/query use. When writing to these formats, a concrete (fixed) SRID is required.

### Data Types Reference

For the full list of supported data types and API usage in Scala, Java, Python, and SQL, see [Data Types](sql-ref-datatypes.html).
