import orjson


def dumps(*args, **kwds):
    return orjson.dumps(*args, **kwds, option=orjson.OPT_UTC_Z).decode("utf-8")


def loads(*args, **kwds):
    return orjson.loads(*args, **kwds)
