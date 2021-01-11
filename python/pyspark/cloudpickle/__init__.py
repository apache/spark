from __future__ import absolute_import


from pyspark.cloudpickle.cloudpickle import *  # noqa
from pyspark.cloudpickle.cloudpickle_fast import CloudPickler, dumps, dump  # noqa

# Conform to the convention used by python serialization libraries, which
# expose their Pickler subclass at top-level under the  "Pickler" name.
Pickler = CloudPickler

__version__ = '1.6.0'
