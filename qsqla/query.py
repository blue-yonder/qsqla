"""
qSQLA Query Syntax
==================

qSQLA is a Query Syntax to filter flat records derived from SQLAlchemy selectable objects.
Each field can be queried with a number of different operators.

The filters are provided in the query string of a ``HTTP GET`` request. The operator is added with a double underscore
to the field name. Unary Operators do not need to specify a value.

.. code::

    GET http://host/vsi/log/deliveries?delivery_id__eq=55&delivery_date__gt=2016-01-01T01:00:00

    Filter:
        delivery_id = 55
        delivery_date > 2016-01-01T01:00:00

    Response:
        [{u'delivery_id': 55,
          u'delivery_category': u'Locations',
          u'delivery_date': u'2016-06-14T06:46:02.296028+00:00',
          u'id': 42,
          u'create_date': u'2016-06-14T06:46:02.296028+00:00',
          u'error_info': None,
          u'row_count': 1,
          u'state': 2,
          u'update_date': u'2016-06-14T06:46:02.296028+00:00'}]

Supported Operators are:

Unary operators:

- ``is_null`` for all fields
- ``is_not_null`` for all fields
- ``is_true`` for Boolean fields
- ``is_false`` for Boolean fields

Binary operators:

- ``eq`` for Integer, String, Date and DateTime fields
- ``ne`` for Integer, String, Date and DateTime fields
- ``ieq`` case insentitive equal for String fields
- ``gt`` for Integer, Date and DateTime fields
- ``lt`` for Integer, Date and DateTime fields
- ``gte`` for Integer, Date and DateTime fields
- ``lte`` for Integer, Date and DateTime fields
- ``like`` for String fields. Case-insensitivity is dependant on the database implementation of LIKE.
- ``not_like`` for String fields. Case-insensitivity is dependant on the database implementation of NOT LIKE.
- ``ilike`` always case-insensitive like for String fields
- ``not_ilike`` always case-insensitive not like for String fields
- ``in`` for Integer, String fields. The values are provided as a comma separated list.
- ``not_in`` for Integer, String fields. The values are provided as a comma separated list.
- ``with`` for relationships. Combine any other binary operator with an additional `__` on any relationship. Can only be used on ORM queries.

Supported Types:

- ``sqlalchemy.types.Integer``
- ``sqlalchemy.types.Boolean``
- ``sqlalchemy.types.Date``
- ``sqlalchemy.types.DateTime``
- ``sqlalchemy.types.String``

In addition to the filters one can provide

- ``_limit`` Limit the query to a number of records.
- ``_offset`` Add an offset to the query.
- ``_order``  The order field.
- ``_desc`` If provided sort in descending order, else in ascending.

"""
import functools

import dateutil.parser
import sqlalchemy
from sqlalchemy.sql.selectable import Selectable


def requires_types(*types):
    def dec(f):
        @functools.wraps(f)
        def wrapper(arg1, arg2=None):
            arg_basetype = getattr(arg1.type, 'impl', arg1.type)
            if not any([isinstance(arg_basetype, t) for t in types]):
                raise TypeError("Cannot apply filter to field {}".format(arg1.name))
            return f(arg1, arg2)
        return wrapper
    return dec


def requires_mapped_attribute(f):
    @functools.wraps(f)
    def wrapper(arg1, arg2=None):
        try:
            getattr(arg1.property, 'mapper')
        except:
            raise TypeError("{} is not a mapped attribute".format(arg1))
        return f(arg1, arg2)
    return wrapper


def convert_type(type_, value):
    cls = type_.__class__
    basetype = getattr(cls, 'impl', cls)
    if issubclass(basetype, sqlalchemy.types.Integer):
        return int(value)
    elif issubclass(basetype, sqlalchemy.types.String):
        return value
    elif issubclass(basetype, sqlalchemy.types.DateTime):
        return dateutil.parser.parse(value)


def get_subquery(arg1, arg2):
    if "=" not in arg2:
        raise KeyError("`any` expects a second operator added with `__`")
    subquery = split_operator(arg2)
    subquery_column = getattr(arg1.property.mapper.class_, subquery[0])
    subquery_op_val = subquery[1].rsplit("=", 1)
    return subquery_column, subquery_op_val


def convert_generic(f):
    @functools.wraps(f)
    def wrapper(arg1, arg2=None):
        return f(arg1, convert_type(arg1.type, arg2))
    return wrapper

def convert_list(f):
    @functools.wraps(f)
    def wrapper(arg1, arg2=None):
        vals = [convert_type(arg1.type, arg.strip()) for arg in arg2.split(",")]
        return f(arg1, vals)
    return wrapper


def is_null(arg1, arg2=None):
    return arg1 == None  # NOQA


def is_not_null(arg1, arg2=None):
    return arg1 != None  # NOQA


@requires_types(sqlalchemy.types.Boolean)
def is_true(arg1, arg2=None):
    return arg1 == True  # NOQA


@requires_types(sqlalchemy.types.Boolean)
def is_false(arg1, arg2=None):
    return arg1 == False  # NOQA


@requires_types(sqlalchemy.types.Integer, sqlalchemy.types.String,
                sqlalchemy.types.DateTime)
@convert_generic
def equals(arg1, arg2):
    return arg1 == arg2


@requires_types(sqlalchemy.types.Integer, sqlalchemy.types.String,
                sqlalchemy.types.DateTime)
@convert_generic
def not_equals(arg1, arg2):
    return arg1 != arg2


@requires_types(sqlalchemy.types.String)
@convert_generic
def ignore_case_equals(arg1, arg2):
    return sqlalchemy.func.lower(arg1) == arg2.lower()


@requires_types(sqlalchemy.types.Integer, sqlalchemy.types.DateTime)
@convert_generic
def greater_than(arg1, arg2):
    return arg1 > arg2


@requires_types(sqlalchemy.types.Integer, sqlalchemy.types.DateTime)
@convert_generic
def greater_than_equals(arg1, arg2):
    return arg1 >= arg2


@requires_types(sqlalchemy.types.Integer, sqlalchemy.types.DateTime)
@convert_generic
def less_than(arg1, arg2):
    return arg1 < arg2


@requires_types(sqlalchemy.types.Integer, sqlalchemy.types.DateTime)
@convert_generic
def less_than_equals(arg1, arg2):
    return arg1 <= arg2


@requires_types(sqlalchemy.types.String)
@convert_generic
def like(arg1, arg2):
    return arg1.like(arg2)


@requires_types(sqlalchemy.types.String)
@convert_generic
def not_like(arg1, arg2):
    return ~arg1.like(arg2)


@requires_types(sqlalchemy.types.String)
@convert_generic
def ilike(arg1, arg2):
    return arg1.ilike(arg2)


@requires_types(sqlalchemy.types.String)
@convert_generic
def not_ilike(arg1, arg2):
    return ~arg1.ilike(arg2)


@requires_types(sqlalchemy.types.Integer, sqlalchemy.types.String)
@convert_list
def in_(arg1, arg2):
    return arg1.in_(arg2)


@requires_types(sqlalchemy.types.Integer, sqlalchemy.types.String)
@convert_list
def not_in(arg1, arg2):
    return ~arg1.in_(arg2)


@requires_mapped_attribute
def with_(arg1, arg2):
    subquery_column, subquery_op_val = get_subquery(arg1, arg2)
    if arg1.property.uselist:
        restriction = arg1.any(OPERATORS[subquery_op_val[0]](subquery_column, subquery_op_val[1]))
    else:
        restriction = arg1.has(OPERATORS[subquery_op_val[0]](subquery_column, subquery_op_val[1]))
    return restriction


UNARY_OPERATORS = ['is_null', 'is_not_null', 'is_true', 'is_false']


OPERATORS = {
    # Unary operators.
    'is_null': is_null,
    'is_not_null': is_not_null,
    'is_true': is_true,
    'is_false': is_false,
    # Binary operators.
    'eq': equals,
    'ne': not_equals,
    'ieq': ignore_case_equals,
    'gt': greater_than,
    'lt': less_than,
    'gte': greater_than_equals,
    'lte': less_than_equals,
    'like': like,
    'not_like': not_like,
    'ilike': ilike,
    'not_ilike': not_ilike,
    'in': in_,
    'not_in': not_in,
    'with': with_
}


def split_operator(param):
    query = param.rsplit('__', 1)
    if len(query) == 1:
        name = query[0]
        operator = 'eq'
    else:
        name = query[0]
        operator = query[1].lower() if (len(query[1]) == 2) else query[1]
    if name == '':
        raise ValueError("No valid parameter provided")
    return (name, operator)


def build_filters(query):
    """ build filter dictionary from a query dict"""
    filters = []
    for key, val in query.items():
        if len(key.rsplit('__', 3)) == 4:
            keys = key.rsplit('__', 3)
            name = keys[0]
            operator = keys[1]
            val = "{}__{}={}".format(keys[2], keys[3], val)
        else:
            name, operator = split_operator(key)
        filters.append({"name": name, "op": operator, "val": val})
    return filters


def get_column(s, name):
    for col in s.columns:
        if col.name.lower() == name.lower():
            return col
    raise KeyError("column {} not found".format(name))


def query(selectable_or_model, filters, limit=None, offset=None, order=None, asc=True):
    """
    Main entry point for applying filters and pagination controls.

    :param selectable_or_model: an SQLAlchemy Core Selectable or ORM Model
    :param filters: a list of filters produced by build_filters
    :param limit: int. the limit.
    :param offset: int. the offset.
    :param order: string. The name of the field to order by.
    :param asc: bool. Ascending (default) or descending order.

    :raises KeyError: if key is not available in query
    :raises ValueError: if value cannot be converted to Column Type
    :raises TypeError: if filter is not available for SQLAlchemy Column Type

    :return: an SQLAlchemy Core Selectable or ORM Query object.
    """
    use_core = isinstance(selectable_or_model, Selectable)
    func = core_query if use_core else orm_query
    filtered = func(selectable_or_model, filters)

    if order:
        if use_core:
            order_col = get_column(selectable_or_model, order)
        else:
            order_col = getattr(selectable_or_model, order)
        if not asc:
            order_col = order_col.desc()
        filtered = filtered.order_by(order_col)

    if limit is None:
        limit = 10000
    else:
        limit = min(int(limit), 10000)

    filtered = filtered.limit(limit)
    if offset:
        filtered = filtered.offset(offset)

    return filtered


def core_query(selectable, filters):
    """Add filters to an sqlalchemy selectable

    :param selectable: the select statements
    :param filters: a list of filters produced by build_filters

    :raises KeyError: if key is not available in query
    :raises ValueError: if value cannot be converted to Column Type
    :raises TypeError: if filter is not available for SQLAlchemy Column Type

    :return: a selectable with the filters applied
    """
    restrictions = []

    alias = selectable.alias("query")

    for f in filters:
        col = get_column(alias, f["name"])
        if f["op"] in UNARY_OPERATORS:
            restrictions.append(OPERATORS[f["op"]](col))
        else:
            restrictions.append(OPERATORS[f["op"]](col, f["val"]))

    if restrictions:
        sel = sqlalchemy.select([alias], whereclause=sqlalchemy.and_(*restrictions))
    else:
        sel = sqlalchemy.select([alias])
    return sel


def orm_query(model, filters):
    """ Add filters to an sqlalchemy ORM query
    :param model: an SQLAlchemy Model
    :param filters: a list of filters produced by build_filters

    :return: a SQLAlchemy ORM Query with the filters applied
    """
    query = sqlalchemy.orm.Query(model)
    restrictions = []
    for f in filters:
        col = getattr(model, f['name'])
        if f["op"] in UNARY_OPERATORS:
            restrictions.append(OPERATORS[f["op"]](col))
        else:
            restrictions.append(OPERATORS[f["op"]](col, f["val"]))
    query = query.filter(*restrictions)
    return query
