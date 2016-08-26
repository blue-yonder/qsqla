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
- ``like`` for String fields
- ``not_like`` for String fields
- ``in`` for Integer, String fields. The values are provided as a comma separated list.
- ``not_in`` for Integer, String fields. The values are provided as a comma separated list.



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
from sqlalchemy import and_, select, func
from sqlalchemy import types
import functools
import dateutil.parser


def requires_types(*types):
    def dec(f):
        @functools.wraps(f)
        def wrapper(arg1, arg2=None):
            if not any([isinstance(arg1.type, t) for t in types]):
                raise TypeError("Cannot apply filter to field {}".format(arg1.name))
            return f(arg1, arg2)
        return wrapper
    return dec


def convert_type(type_, value):
    cls = type_.__class__
    if issubclass(cls, types.Integer):
        return int(value)
    elif issubclass(cls, types.String):
        return value
    elif issubclass(cls, types.DateTime):
        return dateutil.parser.parse(value)


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


@requires_types(types.Boolean)
def is_true(arg1, arg2=None):
    return arg1 == True  # NOQA


@requires_types(types.Boolean)
def is_false(arg1, arg2=None):
    return arg1 == False  # NOQA


@requires_types(types.Integer, types.String, types.DateTime)
@convert_generic
def equals(arg1, arg2):
    return arg1 == arg2


@requires_types(types.Integer, types.String, types.DateTime)
@convert_generic
def not_equals(arg1, arg2):
    return arg1 != arg2


@requires_types(types.String)
@convert_generic
def ignore_case_equals(arg1, arg2):
    return func.lower(arg1) == arg2.lower()

@requires_types(types.Integer, types.DateTime)
@convert_generic
def greater_than(arg1, arg2):
    return arg1 > arg2


@requires_types(types.Integer, types.DateTime)
@convert_generic
def greater_than_equals(arg1, arg2):
    return arg1 >= arg2


@requires_types(types.Integer, types.DateTime)
@convert_generic
def less_than(arg1, arg2):
    return arg1 < arg2


@requires_types(types.Integer, types.DateTime)
@convert_generic
def less_than_equals(arg1, arg2):
    return arg1 <= arg2



@requires_types(types.String)
@convert_generic
def like(arg1, arg2):
    return arg1.like(arg2)


@requires_types(types.String)
@convert_generic
def not_like(arg1, arg2):
    return ~arg1.like(arg2)


@requires_types(types.Integer, types.String)
@convert_list
def in_(arg1, arg2):
    return arg1.in_(arg2)


@requires_types(types.Integer, types.String)
@convert_list
def not_in(arg1, arg2):
    return ~arg1.in_(arg2)


UNRAY_OPERATORS = ['is_null', 'is_not_null', 'is_true', 'is_false']


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
    'in': in_,
    'not_in': not_in,
}



def split_operator(param):
    query = param.rsplit('__', 1)
    if len(query) == 1:
        name = query[0]
        operator = 'eq'
    else:
        name = query[0]
        operator = query[1].lower()
    if name == '':
        raise ValueError("No valid parameter provided")
    return (name, operator)


def build_filters(query):
    """ build filter dictionary from a query dict"""
    filters = []
    for key, val in query.items():
        name, operator = split_operator(key)
        filters.append({"name": name, "op": operator, "val": val})
    return filters


def get_column(s, name):
    for col in s.columns:
        if col.name.lower() == name.lower():
            return col
    raise KeyError("column {} not found".format(name))


def query(selectable, filters, limit=None, offset=None, order=None, asc=True):
    """add filters to an sqlalchemy selactable

    :param selectable: the select statements
    :param filters: a dictionary with filters
    :param limit: the limit
    :param offset: the offset
    :param order: the order field
    :param asc: boolean if sorting should be ascending

    :raises KeyError: if key is not available in query
    :raises ValueError: if value cannont be converted to Column Type
    :raises TypeError: if filter is not available for SQLAlchemy Column Type

    :return: a selectable with the filters, offset and order applied
    """
    restrictions = []

    alias = selectable.alias("query")

    for f in filters:
        col = get_column(alias, f["name"])
        if f["op"] in UNRAY_OPERATORS:
            restrictions.append(OPERATORS[f["op"]](col))
        else:
            restrictions.append(OPERATORS[f["op"]](col, f["val"]))

    if restrictions:
        sel = select([alias], whereclause=and_(*restrictions))
    else:
        sel = select([alias])

    if limit is None:
        limit = 10000
    else:
        limit = min(int(limit), 10000)

    sel = sel.limit(limit)
    if offset:
        sel = sel.offset(offset)
    if order:
        order_col = get_column(alias, order)
        if order_col is None:
            order_col = list(alias.columns)[0]
        if not asc:
            order_col = order_col.desc()
        sel = sel.order_by(order_col)
    return sel




