from pytest import raises

from etl_engine import dialects


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class TestSQL:
    def test_query(self):
        with raises(ValueError):
            dialects.Query()

        query = dialects.Query(from_='schema.table')
        assert query.sql == 'select * from schema.table where 1 = 1'

        query = dialects.Query(
            columns=['first_name', 'last_name', 'dob', 'ssn'],
            from_='credit.user',
        )
        query.where.append('dob > "2000-01-01"')
        query.where.append('dob < "2010-01-01"')
        assert query.sql == (
            'select first_name, last_name, dob, ssn from credit.user where 1 = 1 '
            'and (dob > "2000-01-01") and (dob < "2010-01-01")'
        )

        query = dialects.Query(
            hint='/*+ stream(id) */',
            columns=['*', 'coalesce(create_date, update_date) as business_date'],
            from_='account.balance',
            where=['hwm > 0'],
            other='or hwm < 1000',
        )
        other = (
            'select /*+ stream(id) */ '
            '*, coalesce(create_date, update_date) as business_date '
            'from account.balance '
            'where 1 = 1 and (hwm > 0) or hwm < 1000'
        )
        assert query.sql == other
        assert query == other
        assert query == query

        query = dialects.Query(
            columns=['*', 'coalesce(create_date, update_date) as business_date'],
            from_='account.balance',
            where=['hwm > 0 -- greater than zero'],
            other='or hwm < 1000',
        )
        assert '--' not in query.sql

        query = dialects.Query(
            from_='account.balance',
            where=['hwm > 0; drop table'],
        )
        assert query.sql == 'select * from account.balance where 1 = 1 and (hwm > 0)'
