from datetime import date

from etl_engine import config, metastore


class TestInMemoryMetastore:
    def test_all(self):
        store = metastore.InMemoryMetastore()

        store.hwm = config.HWM.from_literal('user_id', 'max')
        store.hwm = config.HWM.from_literal('user_id', 'richard')
        store.hwm = config.HWM.from_literal('business_date', date(2020, 1, 1))
        store.hwm = config.HWM.from_literal('user_id', 'marvin')

        assert store.hwm == config.HWM.from_literal('user_id', 'marvin')
        assert store.hwm == store.get_hwm('user_id')

        assert store.get_hwm('business_date') == config.HWM.from_literal('business_date', date(2020, 1, 1))

        last_two = [
            config.HWM.from_literal('user_id', 'richard'),
            config.HWM.from_literal('user_id', 'marvin'),
        ]
        assert store.get_hwms('user_id', count=2) == last_two
