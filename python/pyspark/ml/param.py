class Param(object):
    """
    A param with self-contained documentation and optionally default value.
    """

    def __init__(self, parent, name, doc, defaultValue=None):
        self.parent = parent
        self.name = name
        self.doc = doc
        self.defaultValue = defaultValue

    def __str__(self):
        return self.parent + "_" + self.name

    def __repr_(self):
        return self.parent + "_" + self.name
