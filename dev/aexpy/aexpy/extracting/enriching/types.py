import ast
import base64
import logging
from ast import NodeVisitor
from dataclasses import Field, asdict, dataclass, field
from typing import Iterable, Optional

import mypy
from mypy import find_sources
from mypy.dmypy_server import Server
from mypy.infer import infer_function_type_arguments
from mypy.nodes import (ARG_NAMED, ARG_NAMED_OPT, ARG_POS, ARG_STAR, ARG_STAR2,
                        AssignmentStmt, CallExpr, Context, Expression,
                        FuncBase, FuncDef, MemberExpr, MypyFile, NameExpr,
                        Node, RefExpr, ReturnStmt, SymbolNode, SymbolTable,
                        SymbolTableNode, TypeInfo, Var)
from mypy.options import Options
from mypy.traverser import TraverserVisitor
from mypy.types import (AnyType, CallableArgument, CallableType, DeletedType,
                        EllipsisType, ErasedType, Instance, LiteralType,
                        NoneTyp, NoneType, Overloaded, ParamSpecType,
                        PartialType, PlaceholderType, RawExpressionType,
                        StarType, SyntheticTypeVisitor, TupleType, Type,
                        TypeAliasType, TypedDictType, TypeList, TypeOfAny,
                        TypeStrVisitor, TypeType, TypeVarType, UnboundType,
                        UninhabitedType, UnionType, deserialize_type,
                        get_proper_type, is_optional)
from mypy.util import IdMapper
from mypy.version import __version__

from aexpy import json
from aexpy.models import ApiDescription
from aexpy.models import typing as mtyping
from aexpy.models.description import (ApiEntry, AttributeEntry, ClassEntry,
                                      FunctionEntry, ModuleEntry, Parameter,
                                      ParameterKind)
from aexpy.models.description import TypeInfo as MTypeInfo
from aexpy.models.typing import Type as MType
from aexpy.models.typing import TypeFactory

from ..third.mypyserver import PackageMypyServer
from . import Enricher, clearSrc


class TypeTranslateVisitor:
    def visit_all(self, t: Type) -> MType:
        if isinstance(t, LiteralType):
            return self.visit_literal_type(t)
        elif isinstance(t, RawExpressionType):
            return self.visit_raw_expression_type(t)
        elif isinstance(t, PlaceholderType):
            return self.visit_placeholder_type(t)
        elif isinstance(t, TypeAliasType):
            return self.visit_type_alias_type(t)
        elif isinstance(t, TypeType):
            return self.visit_type_type(t)
        elif isinstance(t, UnionType):
            return self.visit_union_type(t)
        elif isinstance(t, TypedDictType):
            return self.visit_typeddict_type(t)
        elif isinstance(t, Overloaded):
            return self.visit_overloaded(t)
        elif isinstance(t, Instance):
            return self.visit_instance(t)
        elif isinstance(t, AnyType):
            return self.visit_any(t)
        elif isinstance(t, NoneType):
            return self.visit_none_type(t)
        elif isinstance(t, UninhabitedType):
            return self.visit_uninhabited_type(t)
        elif isinstance(t, ErasedType):
            return self.visit_erased_type(t)
        elif isinstance(t, DeletedType):
            return self.visit_deleted_type(t)
        elif isinstance(t, UnboundType):
            return self.visit_unbound_type(t)
        elif isinstance(t, TypeVarType):
            return self.visit_type_var(t)
        elif isinstance(t, CallableType):
            return self.visit_callable_type(t)
        elif isinstance(t, TypeList):
            return self.visit_type_list(t)
        elif isinstance(t, CallableArgument):
            return self.visit_callable_argument(t)
        elif isinstance(t, TupleType):
            return self.visit_tuple_type(t)
        elif isinstance(t, ParamSpecType):
            return self.visit_param_spec(t)
        elif isinstance(t, PartialType):
            return self.visit_partial_type(t)
        elif isinstance(t, StarType):
            return self.visit_star_type(t)
        else:
            return TypeFactory.unknown(str(t))

    def visit_unbound_type(self, t: UnboundType) -> MType:
        return TypeFactory.unknown(str(t))
        s = t.name + '?'
        if t.args:
            s += '[{}]'.format(self.list_str(t.args))
        return s

    def visit_type_list(self, t: TypeList) -> MType:
        return TypeFactory.product(*self.list_types(t.items))

    def visit_callable_argument(self, t: CallableArgument) -> MType:
        typ = self.visit_all(t.typ)
        return typ
        if t.name is None:
            return "{}({})".format(t.constructor, typ)
        else:
            return "{}({}, {})".format(t.constructor, typ, t.name)

    def visit_any(self, t: AnyType) -> MType:
        return TypeFactory.any()

    def visit_none_type(self, t: NoneType) -> MType:
        return TypeFactory.none()

    def visit_uninhabited_type(self, t: UninhabitedType) -> MType:
        return TypeFactory.none()

    def visit_erased_type(self, t: ErasedType) -> MType:
        return TypeFactory.unknown(str(t))

    def visit_deleted_type(self, t: DeletedType) -> MType:
        return TypeFactory.unknown(str(t))

    def visit_instance(self, t: Instance) -> MType:
        if t.last_known_value and not t.args:
            # Instances with a literal fallback should never be generic. If they are,
            # something went wrong so we fall back to showing the full Instance repr.
            return TypeFactory.unknown(str(t))
            s = '{}?'.format(t.last_known_value)
        else:
            name = t.type.fullname or t.type.name

            if not name or t.erased:
                return TypeFactory.unknown(str(t))

            baseType = mtyping.ClassType(id=name)
            if t.args:
                return TypeFactory.generic(baseType, *self.list_types(t.args))
            else:
                return baseType

    def visit_type_var(self, t: TypeVarType) -> MType:
        return TypeFactory.any()
        if t.name is None:
            # Anonymous type variable type (only numeric id).
            s = '`{}'.format(t.id)
        else:
            # Named type variable type.
            s = '{}`{}'.format(t.name, t.id)
        if self.id_mapper and t.upper_bound:
            s += '(upper_bound={})'.format(t.upper_bound.accept(self))
        return s

    def visit_param_spec(self, t: ParamSpecType) -> MType:
        return TypeFactory.any()
        if t.name is None:
            # Anonymous type variable type (only numeric id).
            s = f'`{t.id}'
        else:
            # Named type variable type.
            s = f'{t.name_with_suffix()}`{t.id}'
        return s

    def visit_callable_type(self, t: CallableType) -> MType:
        if t.param_spec():
            args = TypeFactory.any()
        else:
            args = TypeFactory.product(*self.list_types(t.arg_types))
        return TypeFactory.callable(args, self.visit_all(t.ret_type))

        param_spec = t.param_spec()
        if param_spec is not None:
            num_skip = 2
        else:
            num_skip = 0

        s = ''
        bare_asterisk = False
        for i in range(len(t.arg_types) - num_skip):
            if s != '':
                s += ', '
            if t.arg_kinds[i].is_named() and not bare_asterisk:
                s += '*, '
                bare_asterisk = True
            if t.arg_kinds[i] == ARG_STAR:
                s += '*'
            if t.arg_kinds[i] == ARG_STAR2:
                s += '**'
            name = t.arg_names[i]
            if name:
                s += name + ': '
            if t.arg_kinds[i].is_optional():
                s += ' ='

        if param_spec is not None:
            n = param_spec.name
            if s:
                s += ', '
            s += f'*{n}.args, **{n}.kwargs'

        s = '({})'.format(s)

        if not isinstance(get_proper_type(t.ret_type), NoneType):
            if t.type_guard is not None:
                s += ' -> TypeGuard[{}]'.format(t.type_guard.accept(self))
            else:
                s += ' -> {}'.format(t.ret_type.accept(self))

        if t.variables:
            vs = []
            for var in t.variables:
                if isinstance(var, TypeVarType):
                    # We reimplement TypeVarType.__repr__ here in order to support id_mapper.
                    if var.values:
                        vals = '({})'.format(', '.join(val.accept(self)
                                                       for val in var.values))
                        vs.append('{} in {}'.format(var.name, vals))
                    elif not is_named_instance(var.upper_bound, 'builtins.object'):
                        vs.append('{} <: {}'.format(
                            var.name, var.upper_bound.accept(self)))
                    else:
                        vs.append(var.name)
                else:
                    # For other TypeVarLikeTypes, just use the name
                    vs.append(var.name)
            s = '{} {}'.format('[{}]'.format(', '.join(vs)), s)

        return 'def {}'.format(s)

    def visit_overloaded(self, t: Overloaded) -> MType:
        return TypeFactory.unknown(str(t))
        a = []
        for i in t.items:
            a.append(i.accept(self))
        return 'Overload({})'.format(', '.join(a))

    def visit_tuple_type(self, t: TupleType) -> MType:
        return TypeFactory.product(*self.list_types(t.items))
        s = self.list_str(t.items)
        if t.partial_fallback and t.partial_fallback.type:
            fallback_name = t.partial_fallback.type.fullname
            if fallback_name != 'builtins.tuple':
                return 'Tuple[{}, fallback={}]'.format(s, t.partial_fallback.accept(self))
        return 'Tuple[{}]'.format(s)

    def visit_typeddict_type(self, t: TypedDictType) -> MType:
        return TypeFactory.unknown(str(t))

        def item_str(name: str, typ: str) -> MType:
            if name in t.required_keys:
                return '{!r}: {}'.format(name, typ)
            else:
                return '{!r}?: {}'.format(name, typ)

        s = '{' + ', '.join(item_str(name, typ.accept(self))
                            for name, typ in t.items.items()) + '}'
        prefix = ''
        if t.fallback and t.fallback.type:
            if t.fallback.type.fullname not in TPDICT_FB_NAMES:
                prefix = repr(t.fallback.type.fullname) + ', '
        return 'TypedDict({}{})'.format(prefix, s)

    def visit_raw_expression_type(self, t: RawExpressionType) -> MType:
        return TypeFactory.literal(repr(t.literal_value))

    def visit_literal_type(self, t: LiteralType) -> MType:
        return TypeFactory.literal(repr(t.value_repr()))

    def visit_star_type(self, t: StarType) -> MType:
        return TypeFactory.unknown(str(t))
        s = t.type.accept(self)
        return '*{}'.format(s)

    def visit_union_type(self, t: UnionType) -> MType:
        return TypeFactory.sum(*self.list_types(t.items))

    def visit_partial_type(self, t: PartialType) -> MType:
        if t.type is None:
            return TypeFactory.none()
            return '<partial None>'
        else:
            return TypeFactory.unknown(str(t))
            return '<partial {}[{}]>'.format(t.type.name,
                                             ', '.join(['?'] * len(t.type.type_vars)))

    def visit_ellipsis_type(self, t: EllipsisType) -> MType:
        return TypeFactory.any()

    def visit_type_type(self, t: TypeType) -> MType:
        return TypeFactory.callable(TypeFactory.any(), self.visit_all(t.item))
        return 'Type[{}]'.format(t.item.accept(self))

    def visit_placeholder_type(self, t: PlaceholderType) -> MType:
        return TypeFactory.unknown(str(t))
        return '<placeholder {}>'.format(t.fullname)

    def visit_type_alias_type(self, t: TypeAliasType) -> MType:
        if t.alias is not None:
            unrolled, recursed = t._partial_expansion()
            return self.visit_all(unrolled)
        return TypeFactory.unknown(str(t))

    def list_types(self, a: Iterable[Type]) -> "list[MType]":
        res = []
        for t in a:
            res.append(self.visit_all(t))
        return res


def encodeType(type: Type | None, logger: "logging.Logger") -> MTypeInfo | None:
    if type is None:
        return None
    try:
        typed = TypeTranslateVisitor().visit_all(type)
        result = type.serialize()
        if isinstance(result, str):
            return MTypeInfo(raw=result, data=result, type=typed, id=str(typed))
        else:
            return MTypeInfo(raw=str(type), data=json.loads(json.dumps(result)), type=typed, id=str(typed))
    except Exception as ex:
        logger.error(f"Failed to encode type {type}.", exc_info=ex)
        return None


class TypeEnricher(Enricher):
    def __init__(self, server: "PackageMypyServer", logger: "logging.Logger | None" = None) -> None:
        super().__init__()
        self.server = server
        self.logger = logger.getChild("type-enrich") if logger is not None else logging.getLogger(
            "type-enrich")

    def enrich(self, api: "ApiDescription") -> None:
        for entry in api.entries.values():
            try:
                match entry:
                    case ModuleEntry() as module:
                        pass
                    case ClassEntry() as cls:
                        pass
                    case FunctionEntry() as func:
                        item = self.server.element(func)
                        if item:
                            type = item[0].type
                            func.type = encodeType(type, self.logger)
                            if isinstance(type, CallableType):
                                func.returnType = encodeType(
                                    type.ret_type, self.logger)
                                for para in func.parameters:
                                    if para.name not in type.arg_names:
                                        continue
                                    typara = type.argument_by_name(para.name)
                                    para.type = encodeType(
                                        typara.typ, self.logger)
                    case AttributeEntry() as attr:
                        item = self.server.element(attr)
                        if item:
                            attrType = None
                            if attr.property:
                                type = item[0].type
                                if isinstance(type, CallableType):
                                    attrType = encodeType(
                                        type.ret_type, self.logger)
                            attr.type = attrType or encodeType(
                                item[0].type, self.logger)
            except Exception as ex:
                self.logger.error(
                    f"Failed to enrich entry {entry.id}.", exc_info=ex)
