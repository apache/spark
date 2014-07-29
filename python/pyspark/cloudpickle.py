"""
This class is defined to override standard pickle functionality

The goals of it follow:
-Serialize lambdas and nested functions to compiled byte code
-Deal with main module correctly
-Deal with other non-serializable objects

It does not include an unpickler, as standard python unpickling suffices.

This module was extracted from the `cloud` package, developed by `PiCloud, Inc.
<http://www.picloud.com>`_.

Copyright (c) 2012, Regents of the University of California.
Copyright (c) 2009 `PiCloud, Inc. <http://www.picloud.com>`_.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the University of California, Berkeley nor the
      names of its contributors may be used to endorse or promote
      products derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""


import operator
import os
import pickle
import struct
import sys
import types
from functools import partial
import itertools
from copy_reg import _extension_registry, _inverted_registry, _extension_cache
import new
import dis
import traceback

#relevant opcodes
STORE_GLOBAL = chr(dis.opname.index('STORE_GLOBAL'))
DELETE_GLOBAL = chr(dis.opname.index('DELETE_GLOBAL'))
LOAD_GLOBAL = chr(dis.opname.index('LOAD_GLOBAL'))
GLOBAL_OPS = [STORE_GLOBAL, DELETE_GLOBAL, LOAD_GLOBAL]

HAVE_ARGUMENT = chr(dis.HAVE_ARGUMENT)
EXTENDED_ARG = chr(dis.EXTENDED_ARG)

import logging
cloudLog = logging.getLogger("Cloud.Transport")

try:
    import ctypes
except (MemoryError, ImportError):
    logging.warning('Exception raised on importing ctypes. Likely python bug.. some functionality will be disabled', exc_info = True)
    ctypes = None
    PyObject_HEAD = None
else:

    # for reading internal structures
    PyObject_HEAD = [
        ('ob_refcnt', ctypes.c_size_t),
        ('ob_type', ctypes.c_void_p),
    ]


try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

# These helper functions were copied from PiCloud's util module.
def islambda(func):
    return getattr(func,'func_name') == '<lambda>'

def xrange_params(xrangeobj):
    """Returns a 3 element tuple describing the xrange start, step, and len
    respectively

    Note: Only guarentees that elements of xrange are the same. parameters may
    be different.
    e.g. xrange(1,1) is interpretted as xrange(0,0); both behave the same
    though w/ iteration
    """

    xrange_len = len(xrangeobj)
    if not xrange_len: #empty
        return (0,1,0)
    start = xrangeobj[0]
    if xrange_len == 1: #one element
        return start, 1, 1
    return (start, xrangeobj[1] - xrangeobj[0], xrange_len)

#debug variables intended for developer use:
printSerialization = False
printMemoization = False

useForcedImports = True #Should I use forced imports for tracking?



class CloudPickler(pickle.Pickler):

    dispatch = pickle.Pickler.dispatch.copy()
    savedForceImports = False
    savedDjangoEnv = False #hack tro transport django environment

    def __init__(self, file, protocol=None, min_size_to_save= 0):
        pickle.Pickler.__init__(self,file,protocol)
        self.modules = set() #set of modules needed to depickle
        self.globals_ref = {}  # map ids to dictionary. used to ensure that functions can share global env

    def dump(self, obj):
        # note: not thread safe
        # minimal side-effects, so not fixing
        recurse_limit = 3000
        base_recurse = sys.getrecursionlimit()
        if base_recurse < recurse_limit:
            sys.setrecursionlimit(recurse_limit)
        self.inject_addons()
        try:
            return pickle.Pickler.dump(self, obj)
        except RuntimeError, e:
            if 'recursion' in e.args[0]:
                msg = """Could not pickle object as excessively deep recursion required.
                Try _fast_serialization=2 or contact PiCloud support"""
                raise pickle.PicklingError(msg)
        finally:
            new_recurse = sys.getrecursionlimit()
            if new_recurse == recurse_limit:
                sys.setrecursionlimit(base_recurse)

    def save_buffer(self, obj):
        """Fallback to save_string"""
        pickle.Pickler.save_string(self,str(obj))
    dispatch[buffer] = save_buffer

    #block broken objects
    def save_unsupported(self, obj, pack=None):
        raise pickle.PicklingError("Cannot pickle objects of type %s" % type(obj))
    dispatch[types.GeneratorType] = save_unsupported

    #python2.6+ supports slice pickling. some py2.5 extensions might as well.  We just test it
    try:
        slice(0,1).__reduce__()
    except TypeError: #can't pickle -
        dispatch[slice] = save_unsupported

    #itertools objects do not pickle!
    for v in itertools.__dict__.values():
        if type(v) is type:
            dispatch[v] = save_unsupported


    def save_dict(self, obj):
        """hack fix
        If the dict is a global, deal with it in a special way
        """
        #print 'saving', obj
        if obj is __builtins__:
            self.save_reduce(_get_module_builtins, (), obj=obj)
        else:
            pickle.Pickler.save_dict(self, obj)
    dispatch[pickle.DictionaryType] = save_dict


    def save_module(self, obj, pack=struct.pack):
        """
        Save a module as an import
        """
        #print 'try save import', obj.__name__
        self.modules.add(obj)
        self.save_reduce(subimport,(obj.__name__,), obj=obj)
    dispatch[types.ModuleType] = save_module    #new type

    def save_codeobject(self, obj, pack=struct.pack):
        """
        Save a code object
        """
        #print 'try to save codeobj: ', obj
        args = (
            obj.co_argcount, obj.co_nlocals, obj.co_stacksize, obj.co_flags, obj.co_code,
            obj.co_consts, obj.co_names, obj.co_varnames, obj.co_filename, obj.co_name,
            obj.co_firstlineno, obj.co_lnotab, obj.co_freevars, obj.co_cellvars
        )
        self.save_reduce(types.CodeType, args, obj=obj)
    dispatch[types.CodeType] = save_codeobject    #new type

    def save_function(self, obj, name=None, pack=struct.pack):
        """ Registered with the dispatch to handle all function types.

        Determines what kind of function obj is (e.g. lambda, defined at
        interactive prompt, etc) and handles the pickling appropriately.
        """
        write = self.write

        name = obj.__name__
        modname = pickle.whichmodule(obj, name)
        #print 'which gives %s %s %s' % (modname, obj, name)
        try:
            themodule = sys.modules[modname]
        except KeyError: # eval'd items such as namedtuple give invalid items for their function __module__
            modname = '__main__'

        if modname == '__main__':
            themodule = None

        if themodule:
            self.modules.add(themodule)

        if not self.savedDjangoEnv:
            #hack for django - if we detect the settings module, we transport it
            django_settings = os.environ.get('DJANGO_SETTINGS_MODULE', '')
            if django_settings:
                django_mod = sys.modules.get(django_settings)
                if django_mod:
                    cloudLog.debug('Transporting django settings %s during save of %s', django_mod, name)
                    self.savedDjangoEnv = True
                    self.modules.add(django_mod)
                    write(pickle.MARK)
                    self.save_reduce(django_settings_load, (django_mod.__name__,), obj=django_mod)
                    write(pickle.POP_MARK)


        # if func is lambda, def'ed at prompt, is in main, or is nested, then
        # we'll pickle the actual function object rather than simply saving a
        # reference (as is done in default pickler), via save_function_tuple.
        if islambda(obj) or obj.func_code.co_filename == '<stdin>' or themodule is None:
            #Force server to import modules that have been imported in main
            modList = None
            if themodule is None and not self.savedForceImports:
                mainmod = sys.modules['__main__']
                if useForcedImports and hasattr(mainmod,'___pyc_forcedImports__'):
                    modList = list(mainmod.___pyc_forcedImports__)
                self.savedForceImports = True
            self.save_function_tuple(obj, modList)
            return
        else:   # func is nested
            klass = getattr(themodule, name, None)
            if klass is None or klass is not obj:
                self.save_function_tuple(obj, [themodule])
                return

        if obj.__dict__:
            # essentially save_reduce, but workaround needed to avoid recursion
            self.save(_restore_attr)
            write(pickle.MARK + pickle.GLOBAL + modname + '\n' + name + '\n')
            self.memoize(obj)
            self.save(obj.__dict__)
            write(pickle.TUPLE + pickle.REDUCE)
        else:
            write(pickle.GLOBAL + modname + '\n' + name + '\n')
            self.memoize(obj)
    dispatch[types.FunctionType] = save_function

    def save_function_tuple(self, func, forced_imports):
        """  Pickles an actual func object.

        A func comprises: code, globals, defaults, closure, and dict.  We
        extract and save these, injecting reducing functions at certain points
        to recreate the func object.  Keep in mind that some of these pieces
        can contain a ref to the func itself.  Thus, a naive save on these
        pieces could trigger an infinite loop of save's.  To get around that,
        we first create a skeleton func object using just the code (this is
        safe, since this won't contain a ref to the func), and memoize it as
        soon as it's created.  The other stuff can then be filled in later.
        """
        save = self.save
        write = self.write

        # save the modules (if any)
        if forced_imports:
            write(pickle.MARK)
            save(_modules_to_main)
            #print 'forced imports are', forced_imports

            forced_names = map(lambda m: m.__name__, forced_imports)
            save((forced_names,))

            #save((forced_imports,))
            write(pickle.REDUCE)
            write(pickle.POP_MARK)

        code, f_globals, defaults, closure, dct, base_globals = self.extract_func_data(func)

        save(_fill_function)  # skeleton function updater
        write(pickle.MARK)    # beginning of tuple that _fill_function expects

        # create a skeleton function object and memoize it
        save(_make_skel_func)
        save((code, len(closure), base_globals))
        write(pickle.REDUCE)
        self.memoize(func)

        # save the rest of the func data needed by _fill_function
        save(f_globals)
        save(defaults)
        save(closure)
        save(dct)
        write(pickle.TUPLE)
        write(pickle.REDUCE)  # applies _fill_function on the tuple

    @staticmethod
    def extract_code_globals(co):
        """
        Find all globals names read or written to by codeblock co
        """
        code = co.co_code
        names = co.co_names
        out_names = set()

        n = len(code)
        i = 0
        extended_arg = 0
        while i < n:
            op = code[i]

            i = i+1
            if op >= HAVE_ARGUMENT:
                oparg = ord(code[i]) + ord(code[i+1])*256 + extended_arg
                extended_arg = 0
                i = i+2
                if op == EXTENDED_ARG:
                    extended_arg = oparg*65536L
                if op in GLOBAL_OPS:
                    out_names.add(names[oparg])
        #print 'extracted', out_names, ' from ', names
        return out_names

    def extract_func_data(self, func):
        """
        Turn the function into a tuple of data necessary to recreate it:
            code, globals, defaults, closure, dict
        """
        code = func.func_code

        # extract all global ref's
        func_global_refs = CloudPickler.extract_code_globals(code)
        if code.co_consts:   # see if nested function have any global refs
            for const in code.co_consts:
                if type(const) is types.CodeType and const.co_names:
                    func_global_refs = func_global_refs.union( CloudPickler.extract_code_globals(const))
        # process all variables referenced by global environment
        f_globals = {}
        for var in func_global_refs:
            #Some names, such as class functions are not global - we don't need them
            if func.func_globals.has_key(var):
                f_globals[var] = func.func_globals[var]

        # defaults requires no processing
        defaults = func.func_defaults

        def get_contents(cell):
            try:
                return cell.cell_contents
            except ValueError, e: #cell is empty error on not yet assigned
                raise pickle.PicklingError('Function to be pickled has free variables that are referenced before assignment in enclosing scope')


        # process closure
        if func.func_closure:
            closure = map(get_contents, func.func_closure)
        else:
            closure = []

        # save the dict
        dct = func.func_dict

        if printSerialization:
            outvars = ['code: ' + str(code) ]
            outvars.append('globals: ' + str(f_globals))
            outvars.append('defaults: ' + str(defaults))
            outvars.append('closure: ' + str(closure))
            print 'function ', func, 'is extracted to: ', ', '.join(outvars)

        base_globals = self.globals_ref.get(id(func.func_globals), {})
        self.globals_ref[id(func.func_globals)] = base_globals

        return (code, f_globals, defaults, closure, dct, base_globals)

    def save_global(self, obj, name=None, pack=struct.pack):
        write = self.write
        memo = self.memo

        if name is None:
            name = obj.__name__

        modname = getattr(obj, "__module__", None)
        if modname is None:
            modname = pickle.whichmodule(obj, name)

        try:
            __import__(modname)
            themodule = sys.modules[modname]
        except (ImportError, KeyError, AttributeError):  #should never occur
            raise pickle.PicklingError(
                "Can't pickle %r: Module %s cannot be found" %
                (obj, modname))

        if modname == '__main__':
            themodule = None

        if themodule:
            self.modules.add(themodule)

        sendRef = True
        typ = type(obj)
        #print 'saving', obj, typ
        try:
            try: #Deal with case when getattribute fails with exceptions
                klass = getattr(themodule, name)
            except (AttributeError):
                if modname == '__builtin__':  #new.* are misrepeported
                    modname = 'new'
                    __import__(modname)
                    themodule = sys.modules[modname]
                    try:
                        klass = getattr(themodule, name)
                    except AttributeError, a:
                        #print themodule, name, obj, type(obj)
                        raise pickle.PicklingError("Can't pickle builtin %s" % obj)
                else:
                    raise

        except (ImportError, KeyError, AttributeError):
            if typ == types.TypeType or typ == types.ClassType:
                sendRef = False
            else: #we can't deal with this
                raise
        else:
            if klass is not obj and (typ == types.TypeType or typ == types.ClassType):
                sendRef = False
        if not sendRef:
            #note: Third party types might crash this - add better checks!
            d = dict(obj.__dict__) #copy dict proxy to a dict
            if not isinstance(d.get('__dict__', None), property): # don't extract dict that are properties
                d.pop('__dict__',None)
            d.pop('__weakref__',None)

            # hack as __new__ is stored differently in the __dict__
            new_override = d.get('__new__', None)
            if new_override:
                d['__new__'] = obj.__new__

            self.save_reduce(type(obj),(obj.__name__,obj.__bases__,
                                   d),obj=obj)
            #print 'internal reduce dask %s %s'  % (obj, d)
            return

        if self.proto >= 2:
            code = _extension_registry.get((modname, name))
            if code:
                assert code > 0
                if code <= 0xff:
                    write(pickle.EXT1 + chr(code))
                elif code <= 0xffff:
                    write("%c%c%c" % (pickle.EXT2, code&0xff, code>>8))
                else:
                    write(pickle.EXT4 + pack("<i", code))
                return

        write(pickle.GLOBAL + modname + '\n' + name + '\n')
        self.memoize(obj)
    dispatch[types.ClassType] = save_global
    dispatch[types.BuiltinFunctionType] = save_global
    dispatch[types.TypeType] = save_global

    def save_instancemethod(self, obj):
        #Memoization rarely is ever useful due to python bounding
        self.save_reduce(types.MethodType, (obj.im_func, obj.im_self,obj.im_class), obj=obj)
    dispatch[types.MethodType] = save_instancemethod

    def save_inst_logic(self, obj):
        """Inner logic to save instance. Based off pickle.save_inst
        Supports __transient__"""
        cls = obj.__class__

        memo  = self.memo
        write = self.write
        save  = self.save

        if hasattr(obj, '__getinitargs__'):
            args = obj.__getinitargs__()
            len(args) # XXX Assert it's a sequence
            pickle._keep_alive(args, memo)
        else:
            args = ()

        write(pickle.MARK)

        if self.bin:
            save(cls)
            for arg in args:
                save(arg)
            write(pickle.OBJ)
        else:
            for arg in args:
                save(arg)
            write(pickle.INST + cls.__module__ + '\n' + cls.__name__ + '\n')

        self.memoize(obj)

        try:
            getstate = obj.__getstate__
        except AttributeError:
            stuff = obj.__dict__
            #remove items if transient
            if hasattr(obj, '__transient__'):
                transient = obj.__transient__
                stuff = stuff.copy()
                for k in list(stuff.keys()):
                    if k in transient:
                        del stuff[k]
        else:
            stuff = getstate()
            pickle._keep_alive(stuff, memo)
        save(stuff)
        write(pickle.BUILD)


    def save_inst(self, obj):
        # Hack to detect PIL Image instances without importing Imaging
        # PIL can be loaded with multiple names, so we don't check sys.modules for it
        if hasattr(obj,'im') and hasattr(obj,'palette') and 'Image' in obj.__module__:
            self.save_image(obj)
        else:
            self.save_inst_logic(obj)
    dispatch[types.InstanceType] = save_inst

    def save_property(self, obj):
        # properties not correctly saved in python
        self.save_reduce(property, (obj.fget, obj.fset, obj.fdel, obj.__doc__), obj=obj)
    dispatch[property] = save_property

    def save_itemgetter(self, obj):
        """itemgetter serializer (needed for namedtuple support)
        a bit of a pain as we need to read ctypes internals"""
        class ItemGetterType(ctypes.Structure):
            _fields_ = PyObject_HEAD + [
                ('nitems', ctypes.c_size_t),
                ('item', ctypes.py_object)
            ]


        obj = ctypes.cast(ctypes.c_void_p(id(obj)), ctypes.POINTER(ItemGetterType)).contents
        return self.save_reduce(operator.itemgetter,
                obj.item if obj.nitems > 1 else (obj.item,))

    if PyObject_HEAD:
        dispatch[operator.itemgetter] = save_itemgetter



    def save_reduce(self, func, args, state=None,
                    listitems=None, dictitems=None, obj=None):
        """Modified to support __transient__ on new objects
        Change only affects protocol level 2 (which is always used by PiCloud"""
        # Assert that args is a tuple or None
        if not isinstance(args, types.TupleType):
            raise pickle.PicklingError("args from reduce() should be a tuple")

        # Assert that func is callable
        if not hasattr(func, '__call__'):
            raise pickle.PicklingError("func from reduce should be callable")

        save = self.save
        write = self.write

        # Protocol 2 special case: if func's name is __newobj__, use NEWOBJ
        if self.proto >= 2 and getattr(func, "__name__", "") == "__newobj__":
            #Added fix to allow transient
            cls = args[0]
            if not hasattr(cls, "__new__"):
                raise pickle.PicklingError(
                    "args[0] from __newobj__ args has no __new__")
            if obj is not None and cls is not obj.__class__:
                raise pickle.PicklingError(
                    "args[0] from __newobj__ args has the wrong class")
            args = args[1:]
            save(cls)

            #Don't pickle transient entries
            if hasattr(obj, '__transient__'):
                transient = obj.__transient__
                state = state.copy()

                for k in list(state.keys()):
                    if k in transient:
                        del state[k]

            save(args)
            write(pickle.NEWOBJ)
        else:
            save(func)
            save(args)
            write(pickle.REDUCE)

        if obj is not None:
            self.memoize(obj)

        # More new special cases (that work with older protocols as
        # well): when __reduce__ returns a tuple with 4 or 5 items,
        # the 4th and 5th item should be iterators that provide list
        # items and dict items (as (key, value) tuples), or None.

        if listitems is not None:
            self._batch_appends(listitems)

        if dictitems is not None:
            self._batch_setitems(dictitems)

        if state is not None:
            #print 'obj %s has state %s' % (obj, state)
            save(state)
            write(pickle.BUILD)


    def save_xrange(self, obj):
        """Save an xrange object in python 2.5
        Python 2.6 supports this natively
        """
        range_params = xrange_params(obj)
        self.save_reduce(_build_xrange,range_params)

    #python2.6+ supports xrange pickling. some py2.5 extensions might as well.  We just test it
    try:
        xrange(0).__reduce__()
    except TypeError: #can't pickle -- use PiCloud pickler
        dispatch[xrange] = save_xrange

    def save_partial(self, obj):
        """Partial objects do not serialize correctly in python2.x -- this fixes the bugs"""
        self.save_reduce(_genpartial, (obj.func, obj.args, obj.keywords))

    if sys.version_info < (2,7): #2.7 supports partial pickling
        dispatch[partial] = save_partial


    def save_file(self, obj):
        """Save a file"""
        import StringIO as pystringIO #we can't use cStringIO as it lacks the name attribute
        from ..transport.adapter import SerializingAdapter

        if not hasattr(obj, 'name') or  not hasattr(obj, 'mode'):
            raise pickle.PicklingError("Cannot pickle files that do not map to an actual file")
        if obj.name == '<stdout>':
            return self.save_reduce(getattr, (sys,'stdout'), obj=obj)
        if obj.name == '<stderr>':
            return self.save_reduce(getattr, (sys,'stderr'), obj=obj)
        if obj.name == '<stdin>':
            raise pickle.PicklingError("Cannot pickle standard input")
        if  hasattr(obj, 'isatty') and obj.isatty():
            raise pickle.PicklingError("Cannot pickle files that map to tty objects")
        if 'r' not in obj.mode:
            raise pickle.PicklingError("Cannot pickle files that are not opened for reading")
        name = obj.name
        try:
            fsize = os.stat(name).st_size
        except OSError:
            raise pickle.PicklingError("Cannot pickle file %s as it cannot be stat" % name)

        if obj.closed:
            #create an empty closed string io
            retval = pystringIO.StringIO("")
            retval.close()
        elif not fsize: #empty file
            retval = pystringIO.StringIO("")
            try:
                tmpfile = file(name)
                tst = tmpfile.read(1)
            except IOError:
                raise pickle.PicklingError("Cannot pickle file %s as it cannot be read" % name)
            tmpfile.close()
            if tst != '':
                raise pickle.PicklingError("Cannot pickle file %s as it does not appear to map to a physical, real file" % name)
        elif fsize > SerializingAdapter.max_transmit_data:
            raise pickle.PicklingError("Cannot pickle file %s as it exceeds cloudconf.py's max_transmit_data of %d" %
                                       (name,SerializingAdapter.max_transmit_data))
        else:
            try:
                tmpfile = file(name)
                contents = tmpfile.read(SerializingAdapter.max_transmit_data)
                tmpfile.close()
            except IOError:
                raise pickle.PicklingError("Cannot pickle file %s as it cannot be read" % name)
            retval = pystringIO.StringIO(contents)
            curloc = obj.tell()
            retval.seek(curloc)

        retval.name = name
        self.save(retval)  #save stringIO
        self.memoize(obj)

    dispatch[file] = save_file
    """Special functions for Add-on libraries"""

    def inject_numpy(self):
        numpy = sys.modules.get('numpy')
        if not numpy or not hasattr(numpy, 'ufunc'):
            return
        self.dispatch[numpy.ufunc] = self.__class__.save_ufunc

    numpy_tst_mods = ['numpy', 'scipy.special']
    def save_ufunc(self, obj):
        """Hack function for saving numpy ufunc objects"""
        name = obj.__name__
        for tst_mod_name in self.numpy_tst_mods:
            tst_mod = sys.modules.get(tst_mod_name, None)
            if tst_mod:
                if name in tst_mod.__dict__:
                    self.save_reduce(_getobject, (tst_mod_name, name))
                    return
        raise pickle.PicklingError('cannot save %s. Cannot resolve what module it is defined in' % str(obj))

    def inject_timeseries(self):
        """Handle bugs with pickling scikits timeseries"""
        tseries = sys.modules.get('scikits.timeseries.tseries')
        if not tseries or not hasattr(tseries, 'Timeseries'):
            return
        self.dispatch[tseries.Timeseries] = self.__class__.save_timeseries

    def save_timeseries(self, obj):
        import scikits.timeseries.tseries as ts

        func, reduce_args, state = obj.__reduce__()
        if func != ts._tsreconstruct:
            raise pickle.PicklingError('timeseries using unexpected reconstruction function %s' % str(func))
        state = (1,
                         obj.shape,
                         obj.dtype,
                         obj.flags.fnc,
                         obj._data.tostring(),
                         ts.getmaskarray(obj).tostring(),
                         obj._fill_value,
                         obj._dates.shape,
                         obj._dates.__array__().tostring(),
                         obj._dates.dtype, #added -- preserve type
                         obj.freq,
                         obj._optinfo,
                         )
        return self.save_reduce(_genTimeSeries, (reduce_args, state))

    def inject_email(self):
        """Block email LazyImporters from being saved"""
        email = sys.modules.get('email')
        if not email:
            return
        self.dispatch[email.LazyImporter] = self.__class__.save_unsupported

    def inject_addons(self):
        """Plug in system. Register additional pickling functions if modules already loaded"""
        self.inject_numpy()
        self.inject_timeseries()
        self.inject_email()

    """Python Imaging Library"""
    def save_image(self, obj):
        if not obj.im and obj.fp and 'r' in obj.fp.mode and obj.fp.name \
            and not obj.fp.closed and (not hasattr(obj, 'isatty') or not obj.isatty()):
            #if image not loaded yet -- lazy load
            self.save_reduce(_lazyloadImage,(obj.fp,), obj=obj)
        else:
            #image is loaded - just transmit it over
            self.save_reduce(_generateImage, (obj.size, obj.mode, obj.tostring()), obj=obj)

    """
    def memoize(self, obj):
        pickle.Pickler.memoize(self, obj)
        if printMemoization:
            print 'memoizing ' + str(obj)
    """



# Shorthands for legacy support

def dump(obj, file, protocol=2):
    CloudPickler(file, protocol).dump(obj)

def dumps(obj, protocol=2):
    file = StringIO()

    cp = CloudPickler(file,protocol)
    cp.dump(obj)

    #print 'cloud dumped', str(obj), str(cp.modules)

    return file.getvalue()


#hack for __import__ not working as desired
def subimport(name):
    __import__(name)
    return sys.modules[name]

#hack to load django settings:
def django_settings_load(name):
    modified_env = False

    if 'DJANGO_SETTINGS_MODULE' not in os.environ:
        os.environ['DJANGO_SETTINGS_MODULE'] = name # must set name first due to circular deps
        modified_env = True
    try:
        module = subimport(name)
    except Exception, i:
        print >> sys.stderr, 'Cloud not import django settings %s:' % (name)
        print_exec(sys.stderr)
        if modified_env:
            del os.environ['DJANGO_SETTINGS_MODULE']
    else:
        #add project directory to sys,path:
        if hasattr(module,'__file__'):
            dirname = os.path.split(module.__file__)[0] + '/'
            sys.path.append(dirname)

# restores function attributes
def _restore_attr(obj, attr):
    for key, val in attr.items():
        setattr(obj, key, val)
    return obj

def _get_module_builtins():
    return pickle.__builtins__

def print_exec(stream):
    ei = sys.exc_info()
    traceback.print_exception(ei[0], ei[1], ei[2], None, stream)

def _modules_to_main(modList):
    """Force every module in modList to be placed into main"""
    if not modList:
        return

    main = sys.modules['__main__']
    for modname in modList:
        if type(modname) is str:
            try:
                mod = __import__(modname)
            except Exception, i: #catch all...
                sys.stderr.write('warning: could not import %s\n.  Your function may unexpectedly error due to this import failing; \
A version mismatch is likely.  Specific error was:\n' % modname)
                print_exec(sys.stderr)
            else:
                setattr(main,mod.__name__, mod)
        else:
            #REVERSE COMPATIBILITY FOR CLOUD CLIENT 1.5 (WITH EPD)
            #In old version actual module was sent
            setattr(main,modname.__name__, modname)

#object generators:
def _build_xrange(start, step, len):
    """Built xrange explicitly"""
    return xrange(start, start + step*len, step)

def _genpartial(func, args, kwds):
    if not args:
        args = ()
    if not kwds:
        kwds = {}
    return partial(func, *args, **kwds)


def _fill_function(func, globals, defaults, closure, dict):
    """ Fills in the rest of function data into the skeleton function object
        that were created via _make_skel_func().
         """
    func.func_globals.update(globals)
    func.func_defaults = defaults
    func.func_dict = dict

    if len(closure) != len(func.func_closure):
        raise pickle.UnpicklingError("closure lengths don't match up")
    for i in range(len(closure)):
        _change_cell_value(func.func_closure[i], closure[i])

    return func

def _make_skel_func(code, num_closures, base_globals = None):
    """ Creates a skeleton function object that contains just the provided
        code and the correct number of cells in func_closure.  All other
        func attributes (e.g. func_globals) are empty.
    """
    #build closure (cells):
    if not ctypes:
        raise Exception('ctypes failed to import; cannot build function')

    cellnew = ctypes.pythonapi.PyCell_New
    cellnew.restype = ctypes.py_object
    cellnew.argtypes = (ctypes.py_object,)
    dummy_closure = tuple(map(lambda i: cellnew(None), range(num_closures)))

    if base_globals is None:
        base_globals = {}
    base_globals['__builtins__'] = __builtins__

    return types.FunctionType(code, base_globals,
                              None, None, dummy_closure)

# this piece of opaque code is needed below to modify 'cell' contents
cell_changer_code = new.code(
    1, 1, 2, 0,
    ''.join([
        chr(dis.opmap['LOAD_FAST']), '\x00\x00',
        chr(dis.opmap['DUP_TOP']),
        chr(dis.opmap['STORE_DEREF']), '\x00\x00',
        chr(dis.opmap['RETURN_VALUE'])
    ]),
    (), (), ('newval',), '<nowhere>', 'cell_changer', 1, '', ('c',), ()
)

def _change_cell_value(cell, newval):
    """ Changes the contents of 'cell' object to newval """
    return new.function(cell_changer_code, {}, None, (), (cell,))(newval)

"""Constructors for 3rd party libraries
Note: These can never be renamed due to client compatibility issues"""

def _getobject(modname, attribute):
    mod = __import__(modname, fromlist=[attribute])
    return mod.__dict__[attribute]

def _generateImage(size, mode, str_rep):
    """Generate image from string representation"""
    import Image
    i = Image.new(mode, size)
    i.fromstring(str_rep)
    return i

def _lazyloadImage(fp):
    import Image
    fp.seek(0)  #works in almost any case
    return Image.open(fp)

"""Timeseries"""
def _genTimeSeries(reduce_args, state):
    import scikits.timeseries.tseries as ts
    from numpy import ndarray
    from numpy.ma import MaskedArray


    time_series = ts._tsreconstruct(*reduce_args)

    #from setstate modified
    (ver, shp, typ, isf, raw, msk, flv, dsh, dtm, dtyp, frq, infodict) = state
    #print 'regenerating %s' % dtyp

    MaskedArray.__setstate__(time_series, (ver, shp, typ, isf, raw, msk, flv))
    _dates = time_series._dates
    #_dates.__setstate__((ver, dsh, typ, isf, dtm, frq))  #use remote typ
    ndarray.__setstate__(_dates,(dsh,dtyp, isf, dtm))
    _dates.freq = frq
    _dates._cachedinfo.update(dict(full=None, hasdups=None, steps=None,
                                   toobj=None, toord=None, tostr=None))
    # Update the _optinfo dictionary
    time_series._optinfo.update(infodict)
    return time_series

