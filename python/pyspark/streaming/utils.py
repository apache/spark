__author__ = 'ktakagiw'

def msDurationToString(ms):
    """
    Returns a human-readable string representing a duration such as "35ms"
    """
    second = 1000
    minute = 60 * second
    hour = 60 * minute

    if ms < second:
        return "%d ms" % ms
    elif ms < minute:
        return "%.1f s" % (float(ms) / second)
    elif ms < hout:
        return "%.1f m" % (float(ms) / minute)
    else:
        return "%.2f h" % (float(ms) / hour)
