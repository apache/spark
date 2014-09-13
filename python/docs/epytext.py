import re


RULES = (
#    (r"@param (\w+):", r":para "),
#    (r"@type (\w+):", ""),
#    (r"@return:", ""),
#    (r"@rtype:", ""),
#    (r"@keyword (\w+):", ""),
#    (r"@raise (\w+):", ""),
#    (r"@deprecated:", ""),
    (r"<[\w.]+>", r""),
    (r"L{([\w.()]+)}", r":class:`\1`"),
    (r"[LC]{(\w+\.\w+)\(\)}", r":func:`\1`"),
    (r"C{([\w.()]+)}", r":class:`\1`"),
    (r"[IBCM]{(.+)}", r"`\1`"),
    ('pyspark.rdd.RDD', 'RDD'),
)

def _convert_epytext(line):
    """
    >>> _convert_epytext("L{A}")
    :class:`A`
    """
    line = line.replace('@', ':')
    for p, sub in RULES:
        line = re.sub(p, sub, line)
    return line

def _process_docstring(app, what, name, obj, options, lines):
    for i in range(len(lines)):
        lines[i] = _convert_epytext(lines[i])


def setup(app):
    app.connect("autodoc-process-docstring", _process_docstring)
