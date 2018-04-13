import unittest
import os

from datetime import date, datetime, timedelta
from operator import itemgetter
from sqlalchemy import (MetaData, Table, Column, DateTime, Integer, String,
                        ForeignKey, create_engine, types, select)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

import qsqla.query as qsqla


class CustomDateTime(types.TypeDecorator):
    impl = DateTime


Base = declarative_base()


user_pet = Table('user_pet',
                 Base.metadata,
                 Column('user_u_id', Integer, ForeignKey('user_table.u_id')),
                 Column('pet_p_id', Integer, ForeignKey('pet.p_id'))
                 )

class Location(Base):
    __tablename__ = 'location'
    l_id = Column(Integer, primary_key=True)
    l_name = Column(String(16))
    l_date = Column(CustomDateTime)


class User(Base):
    __tablename__ = 'user_table'
    u_id = Column(Integer, primary_key=True)
    u_name = Column(String(16))
    u_l_id = Column(ForeignKey(Location.l_id))
    location = relationship(Location)
    pets = relationship("Pet", secondary=user_pet)
    u_date = Column(CustomDateTime)


class Pet(Base):
    __tablename__ = 'pet'
    p_id = Column(Integer, primary_key=True)
    p_name = Column(String(16))
    p_date = Column(CustomDateTime)


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
        dsn = os.environ.get("ENV_DSN", "sqlite:///:memory:")
        self.engine = create_engine(dsn)
        self.db = self.engine.connect()
        self.session = sessionmaker()(bind=self.db)
        self.now = datetime.now()
        Base.metadata.create_all(self.db)
        self.user = User.__table__
        self.location = Location.__table__
        l1 = Location(l_name='Karlsruhe', l_date=self.now)
        l2 = Location(l_name='Stuttgart', l_date=self.now)
        self.session.add(l1)
        self.session.add(l2)
        p1 = Pet(p_name="Beethoven", p_date=self.now)
        p2 = Pet(p_name="Hooch", p_date=self.now)
        p3 = Pet(p_name="Sissy", p_date=self.now)
        self.session.add(p1)
        self.session.add(p2)
        self.session.add(p3)
        u1 = User(u_name='Micha', location=l1, u_date=self.now)
        u2 = User(u_name='Oli', location=l1, u_date=self.now)
        u3 = User(u_name='Tom', location=l2, u_date=self.now)
        u1.pets.append(p1)
        u1.pets.append(p2)
        u2.pets.append(p2)
        u3.pets.append(p3)
        self.session.add(u1)
        self.session.add(u2)
        self.session.add(u3)
        self.session.commit()
        self.joined_select = self.location.join(
            self.user, self.location.c.l_id == self.user.c.u_l_id).select()

    def tearDown(self):
        Base.metadata.drop_all(self.db)
        self.session.commit()
        self.session.close()
        self.db.close()


class TestSqlaQueryCore(DBTestCase):
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
        limit = 2
        query = qsqla.query(self.user.select(), [], limit=limit)
        rows = self.db.execute(query)
        self.assertEquals(len(list(rows)), limit)

    def test_offset(self):
        query = qsqla.query(self.user.select(), [], offset=2)
        rows = self.db.execute(query)
        self.assertEquals(len(list(rows)), 1)

    def test_order_with_default_ascending(self):
        query = qsqla.query(self.user.select(), [], order="u_id")
        rows = self.db.execute(query)
        self.assertEquals([row.u_id for row in rows], [1, 2, 3])

    def test_order_with_forced_ascending(self):
        query = qsqla.query(self.user.select(), [], order="u_id", asc=True)
        rows = self.db.execute(query)
        self.assertEquals([row.u_id for row in rows], [1, 2, 3])

    def test_order_with_forced_descending(self):
        query = qsqla.query(self.user.select(), [], order="u_id", asc=False)
        rows = self.db.execute(query)
        self.assertEquals([row.u_id for row in rows], [3, 2, 1])


class TestSqlaQueryORM(DBTestCase):

    def test_limit(self):
        query = qsqla.query(User, [], limit=2)
        query.session = self.session
        rows = query.all()
        self.assertEquals(len(list(rows)), 2)

    def test_offset(self):
        query = qsqla.query(User, [], offset=2)
        query.session = self.session
        rows = query.all()
        self.assertEquals(len(list(rows)), 1)

    def test_order_with_default_ascending(self):
        query = qsqla.query(User, [], order="u_id")
        query.session = self.session
        rows = query.all()
        self.assertEquals([row.u_id for row in rows], [1, 2, 3])

    def test_order_with_forced_ascending(self):
        query = qsqla.query(User, [], order="u_id", asc=True)
        query.session = self.session
        rows = query.all()
        self.assertEquals([row.u_id for row in rows], [1, 2, 3])

    def test_order_with_forced_descending(self):
        query = qsqla.query(self.user.select(), [], order="u_id", asc=False)
        rows = self.db.execute(query)
        self.assertEquals([row.u_id for row in rows], [3, 2, 1])


class TestOperators(DBTestCase):
    def perform_assertion(self, filter, expected_names):
        # test core
        selectable = qsqla.query(self.joined_select, [filter])
        rows = self.db.execute(selectable)
        self.assertEqual([dict(r)['u_name'] for r in rows], expected_names)

        if filter['name'].startswith('u_'):
            # test ORM
            q = qsqla.query(User, [filter])
            q.session = self.session
            self.assertEqual([row.u_name for row in q.all()], expected_names)

    def test_is_null(self):
        self.perform_assertion({"name": "l_id", "op": "is_null"}, [])
        self.perform_assertion({"name": "u_id", "op": "is_null"}, [])

    def test_is_not_null(self):
        self.perform_assertion({"name": "l_id", "op": "is_not_null"},
                          ['Micha', 'Oli', 'Tom'])
        self.perform_assertion({"name": "u_id", "op": "is_not_null"},
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
        self.perform_assertion({"name": "u_name", "op": "like", "val": "%om%"},
            ['Tom'])


    # Fails if run with sqlite, see issue https://www.sqlite.org/pragma.html#pragma_case_sensitive_like
    def test_like_is_case_sensitive(self):
        self.perform_assertion(
            {"name": "l_name", "op": "like", "val": "%gaRT%"},
            [])
        self.perform_assertion(
            {"name": "u_name", "op": "like", "val": "%OM%"},
            [])

    def test_not_like(self):
        self.perform_assertion({"name": "l_name", "op": "not_like", "val": "%gart%"},
                          ['Micha', 'Oli'])
        self.perform_assertion(
            {"name": "u_name", "op": "not_like", "val": "%om%"},
            ['Micha', 'Oli'])

    def test_ilike(self):
        self.perform_assertion(
            {"name": "l_name", "op": "ilike", "val": "%gaRT%"},
            ['Tom'])
        self.perform_assertion(
            {"name": "u_name", "op": "ilike", "val": "%oM%"},
            ['Tom'])

    def test_not_ilike(self):
        self.perform_assertion({"name": "l_name", "op": "not_ilike", "val": "%gaRT%"},
                          ['Micha', 'Oli'])
        self.perform_assertion(
            {"name": "u_name", "op": "not_ilike", "val": "%oM%"},
            ['Micha', 'Oli'])

    def test_in_(self):
        self.perform_assertion({"name": "u_id", "op": "in", "val": "1,3"},
                          ['Micha', 'Tom'])

    def test_not_in(self):
        self.perform_assertion({"name": "u_id", "op": "not_in", "val": "1,3"},
                          ['Oli'])

    def test_operation_on_typedecorated_type(self):
        val = (datetime.now() - timedelta(hours=5)).isoformat()
        self.perform_assertion({"name": "u_date", "op": "ne", "val": val},
                               ['Micha', 'Oli', 'Tom'])

    def test_greater_than_typedecorated_datetime(self):
        datestring = (self.now - timedelta(minutes=1)).isoformat()
        self.perform_assertion({"name": "u_date", "op": "gt", "val": datestring},
                          ['Micha', 'Oli', 'Tom'])
        datestring = (self.now + timedelta(minutes=1)).isoformat()
        self.perform_assertion(
            {"name": "l_date", "op": "gt", "val": datestring},
            [])

    def test_with_eq_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_name__eq=Hooch"}], order="u_id", asc=True)
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_ne_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_id__ne=3"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_ieq_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_name__ieq=beethoven"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha'])

    def test_with_gt_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_id__gt=2"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Tom'])

    def test_with_lt_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_id__lt=2"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha'])

    def test_with_gte_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_id__gte=2"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli', 'Tom'])

    def test_with_lte_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_id__lte=2"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_like_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_name__like=%eeth%"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha'])

    def test_with_not_like_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_name__not_like=%issy"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_ilike_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_name__ilike=%eeth%"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha'])

    def test_with_not_ilike_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_name__not_ilike=%issy"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_in_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_name__in=Beethoven,Sissy"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Tom'])

    def test_with_not_in_many_relation(self):
        q = qsqla.query(User, [{"name": "pets", "op": "with", "val": "p_name__not_in=Hooch,Sissy"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha'])

    def test_with_eq_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_name__eq=Stuttgart"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Tom'])

    def test_with_ne_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_id__ne=1"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Tom'])

    def test_with_ieq_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_name__ieq=karlsruhe"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_gt_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_id__gt=1"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Tom'])

    def test_with_lt_Scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_id__lt=2"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_gte_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_id__gte=2"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Tom'])

    def test_with_lte_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_id__lte=1"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_like_location_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_name__like=%uttg%"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Tom'])

    def test_with_not_like_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_name__not_like=%uttga%"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_ilike_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_name__ilike=%ArlSr%"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_not_ilike_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_name__not_ilike=%uttgart"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])

    def test_with_in_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_name__in=Karlsruhe,Stuttgart"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli', 'Tom'])

    def test_with_not_in_scalar_relation(self):
        q = qsqla.query(User, [{"name": "location", "op": "with", "val": "l_name__not_in=Stuttgart"}])
        q.session = self.session
        self.assertEqual([row.u_name for row in q.all()], ['Micha', 'Oli'])
