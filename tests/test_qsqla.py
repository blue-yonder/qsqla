from datetime import date, datetime
import unittest

from sqlalchemy import (MetaData, Table, Column, DateTime, Integer, String,
                        ForeignKey, create_engine, select)

from operator import itemgetter

import qsqla.query as qsqla


class TestSqlaSplitOperator(unittest.TestCase):
    def test_without_operator(self):
        param = "field"
        name, operator = qsqla.split_operator(param)
        self.assertEquals(name, param)
        self.assertEquals(operator, "eq")

    def test_with_operator(self):
        param = "field__eq"
        name, operator = qsqla.split_operator(param)
        self.assertEquals(name, "field")
        self.assertEquals(operator, "eq")


    def test_with_operator_fancy_name(self):
        param = "field_with__underscore__eq"
        name, operator = qsqla.split_operator(param)
        self.assertEquals(name, "field_with__underscore")
        self.assertEquals(operator, "eq")


class TestSqlaFilter(unittest.TestCase):
    def test_empty(self):
        f = {}
        filters = qsqla.build_filters(f)
        self.assertEquals(filters, [])

    def test_one_without_operator(self):
        f = {"age": 55}
        filters = qsqla.build_filters(f)
        self.assertEquals(filters, [{"name": "age", "op": "eq", "val": 55}])

    def test_one_with_operator(self):
        f = {"age__gt": 55}
        filters = qsqla.build_filters(f)
        self.assertEquals(filters, [{"name": "age", "op": "gt", "val": 55}])

    def test_multiple(self):
        f = {"age__eq": 55, "surname__like": "%joe%"}
        filters = qsqla.build_filters(f)
        expected = [{"name": "surname", "op": "like", "val": "%joe%"},
                    {"name": "age", "op": "eq", "val": 55}]

        self.assertEquals(sorted(filters, key=itemgetter("name")), 
                          sorted(expected, key=itemgetter("name")))


class DBTestCase(unittest.TestCase):
    def setUp(self):
        self.db = create_engine("sqlite://")

        self.md = MetaData(self.db)
        self.user = Table('user', self.md,
                          Column('u_id', Integer, primary_key=True),
                          Column('u_name', String(16), nullable=False),
                          Column('u_l_id', Integer,
                                 ForeignKey("location.l_id"))
                          )

        self.location = Table('location', self.md,
                              Column('l_id', Integer, primary_key=True),
                              Column('l_name', String(16), nullable=False),
                              Column('l_date', DateTime)
                              )
        self.md.create_all()

        self.db.execute(
            "insert into location values(1, 'Karlsruhe', '{}')".format(
                datetime.now()))
        self.db.execute(
            "insert into location values(2, 'Stuttgart', '{}')".format(
                datetime.now()))
        self.db.execute("insert into user values(1, 'Micha', 1)")
        self.db.execute("insert into user values(2, 'Oli', 1)")
        self.db.execute("insert into user values(3, 'Tom', 2)")
        self.joined_select = self.location.join(
            self.user, self.location.c.l_id == self.user.c.u_l_id).select()


class TestSqlaQuery(DBTestCase):
    def assertEqualColumns(self, first, second):
        first_columns = set(c.name for c in first.columns)
        second_columns = set(c.name for c in second.columns)
        self.assertEquals(first_columns, second_columns)

    def test_no_filters(self):
        sel = self.location.select()
        result = qsqla.query(sel, [])
        self.assertEqualColumns(sel, result)
        self.assertIsNone(result._whereclause)

    def test_single_filter(self):
        result = qsqla.query(self.joined_select, [{"name": "l_id", "op": "eq", "val": 1}])
        self.assertEqualColumns(self.joined_select, result)

    def test_multiple_filters(self):
        filters = [{"name": "l_id", "op": "eq", "val": 1},
                   {"name": "u_id", "op": "eq", "val": 1} ]

        result = qsqla.query(self.joined_select, filters)
        self.assertEqualColumns(self.joined_select, result)

    def test_limit(self):
        query = qsqla.query(self.user.select(),
                            [],
                            limit=2)
        rows = self.db.execute(query)

        self.assertEquals(len(list(rows)), 2)

    def test_offset(self):
        query = qsqla.query(self.user.select(),
                            [],
                            offset=2)
        rows = self.db.execute(query)

        remaining_rows = 3 - 2
        self.assertEquals(len(list(rows)), remaining_rows)

    def test_order_with_default_ascending(self):
        query = qsqla.query(self.user.select(),
                            [],
                            order="u_id")
        rows = self.db.execute(query)
        self.assertEquals([row.u_id for row in rows], [1, 2, 3])

    def test_order_with_forced_ascending(self):
        query = qsqla.query(self.user.select(),
                            [],
                            order="u_id",
                            asc=True)
        rows = self.db.execute(query)
        self.assertEquals([row.u_id for row in rows], [1, 2, 3])

    def test_order_with_forced_ascending(self):
        query = qsqla.query(self.user.select(),
                            [],
                            order="u_id",
                            asc=False)
        rows = self.db.execute(query)
        self.assertEquals([row.u_id for row in rows], [3, 2, 1])


class TestOperators(DBTestCase):
    def perform_assertion(self, filter, expected_names):
        selectable = qsqla.query(self.joined_select, [filter])
        rows = self.db.execute(selectable)
        self.assertEqual([dict(r)['u_name'] for r in rows], expected_names)

    def test_is_null(self):
        self.perform_assertion({"name": "l_id", "op": "is_null"}, [])

    def test_is_not_null(self):
        self.perform_assertion({"name": "l_id", "op": "is_not_null"},
                          ['Micha', 'Oli', 'Tom'])

    def test_equals(self):
        self.perform_assertion({"name": "l_id", "op": "eq", "val": "1"},
                          ['Micha', 'Oli'])
        self.perform_assertion({"name": "u_name", "op": "eq", "val": "Oli"},
                          ['Oli'])
        self.perform_assertion({"name": "u_name", "op": "eq", "val": "Hannes"},
                          [])

    def test_ignore_case_equals(self):
        self.perform_assertion({"name": "u_name", "op": "ieq", "val": "Oli"},
                               ['Oli'])
        self.perform_assertion({"name": "u_name", "op": "ieq", "val": "oli"},
                               ['Oli'])
        self.perform_assertion({"name": "u_name", "op": "ieq", "val": "Hannes"},
                               [])

    def test_not_equals(self):
        self.perform_assertion({"name": "l_id", "op": "ne", "val": "1"},
                          ['Tom'])
        self.perform_assertion({"name": "u_name", "op": "ne", "val": "Oli"},
                          ['Micha', 'Tom'])
        self.perform_assertion({"name": "u_name", "op": "ne", "val": "Hannes"},
                          ['Micha', 'Oli', 'Tom'])

    def test_greater_than(self):
        self.perform_assertion({"name": "u_id", "op": "gt", "val": "1"},
                          ['Oli', 'Tom'])
        self.perform_assertion({"name": "u_id", "op": "gt", "val": "3"},
                          [])

    def test_greater_than_equals(self):
        self.perform_assertion({"name": "u_id", "op": "gte", "val": "1"},
                          ['Micha', 'Oli', 'Tom'])

    def test_less_than(self):
        self.perform_assertion({"name": "u_id", "op": "lt", "val": "1"},
                          [])
        self.perform_assertion({"name": "u_id", "op": "lt", "val": "2"},
                          ['Micha'])

    def test_less_than_equals(self):
        self.perform_assertion({"name": "u_id", "op": "lte", "val": "2"},
                          ['Micha', 'Oli'])

    def test_like(self):
        self.perform_assertion({"name": "l_name", "op": "like", "val": "%gart%"},
                          ['Tom'])

    def test_not_like(self):
        self.perform_assertion({"name": "l_name", "op": "not_like", "val": "%gart%"},
                          ['Micha', 'Oli'])

    def test_in_(self):
        self.perform_assertion({"name": "u_id", "op": "in", "val": "1,3"},
                          ['Micha', 'Tom'])

    def test_not_in(self):
        self.perform_assertion({"name": "u_id", "op": "not_in", "val": "1,3"},
                          ['Oli'])

