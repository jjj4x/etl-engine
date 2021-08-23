from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, date
from numbers import Number
from typing import Any, List, Optional, MutableMapping

HWM_DISPATCH_TABLE = {
    'int': int,
    'float': float,
    'datetime': datetime,
    'str': str,
}
HWM_INIT_VALUES = {
    'int': 0,
    'float': 0,
    'datetime': datetime(1900, 1, 1),
    'str': '',
}
HWM_TYPES = set(HWM_DISPATCH_TABLE.values())


# noinspection SqlDialectInspection
@dataclass(init=False)
class HWM:
    def __init__(
        self,
        expression: str,
        value: Optional[str],
        literal_value: Any,
        hwm_type: str = 'str',
    ):
        self.expression = expression
        self._value = value
        self._literal_value = literal_value
        self.hwm_type = hwm_type

        self.initialize()

        expected_value, expected_literal, _ = self.cast_literal(
            self.literal_value,
            self.hwm_type,
        )
        if (
            self.value != expected_value
            or self.literal_value != expected_literal
        ):
            raise ValueError('The value and literal_value should match.')

        if self.hwm_type not in HWM_DISPATCH_TABLE:
            raise TypeError(f'The hwm_type should be one of: "{HWM_DISPATCH_TABLE}".')

    @property
    def predicate_right(self):
        return f'{self.expression} <= {self.value}'  # user_id <= 1000

    @property
    def predicate_left(self):
        return f'{self.value} < {self.expression}'  # 500 < user_id

    @property
    def is_unset(self):
        return self.literal_value is None

    def initialize(self):
        if self.is_unset:
            self.literal_value = HWM_INIT_VALUES[self.hwm_type]

    @property
    def literal_value(self):
        return self._literal_value

    @literal_value.setter
    def literal_value(self, new_value):
        value, literal_value, _ = self.cast_literal(new_value, hwm_type=self.hwm_type)
        self._value = value
        self._literal_value = literal_value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        raise AttributeError(
            'The HWM.value should not be set after the instantiation.'
            ' Use HWM.literal_value instead.'
        )

    def max_hwm_sql(self, fqdn):
        return f'select max({self.expression}) as new_hwm from {fqdn}'

    @classmethod
    def from_mapping(cls, **mapping):
        hwm_type = mapping['hwm_type']
        value = mapping.get('value')

        if value is None:
            return cls.from_literal(
                mapping['expression'],
                literal_value=None,  # NOTE: The HWM is unset/should be checked.
                hwm_type=hwm_type,
            )

        if hwm_type != 'datetime':
            literal_value = HWM_DISPATCH_TABLE[hwm_type](value)
        else:
            literal_value = datetime.fromisoformat(value.strip('"'))

        mapping.setdefault('literal_value', literal_value)

        return cls.from_literal(mapping['expression'], mapping['literal_value'])

    @classmethod
    def from_literal(cls, expression, literal_value, hwm_type=None):
        return cls(expression, *cls.cast_literal(literal_value, hwm_type))

    @classmethod
    def cast_literal(cls, literal_value, hwm_type):
        if isinstance(literal_value, date):
            literal_value = datetime(literal_value.year, literal_value.month, literal_value.day)

        if hwm_type is not None and hwm_type not in HWM_DISPATCH_TABLE:
            raise ValueError(f'The hwm_type should be one of: {HWM_DISPATCH_TABLE}.')

        if literal_value is None and hwm_type is None:
            raise TypeError('Specify hwm_type explicitly for an undefined literal_value.')
        if literal_value is None:
            value = literal_value
        elif isinstance(literal_value, datetime):
            value = f'"{literal_value.isoformat()}"'
            hwm_type = 'datetime'
        elif isinstance(literal_value, Number) and literal_value.__class__ in HWM_TYPES:
            value = str(literal_value)
            hwm_type = literal_value.__class__.__name__
        else:
            value = str(literal_value)
            hwm_type = 'str'

        return value, literal_value, hwm_type

    def as_mapping(self):
        return {
            'expression': self.expression,
            'value': self.value,
            'literal_value': self.literal_value,
            'hwm_type': self.hwm_type,
        }


class MetastoreABC(ABC):
    @abstractmethod
    def hwms(self):
        """Return a slice of last HWMs."""

    @property
    @abstractmethod
    def hwm(self) -> Optional[HWM]:
        """Return last HWM."""

    @hwm.setter
    @abstractmethod
    def hwm(self, value: HWM) -> List[HWM]:
        """Set new last HWM."""

    @abstractmethod
    def set_hwm(self, key, literal_value) -> HWM:
        """Set new last HWM from the literal_value."""

    @abstractmethod
    def get_hwm(self, key) -> HWM:
        """Get last HWM by key."""

    @abstractmethod
    def get_hwms(self, key, count=1):
        """Get a slice of last HWMs by key."""


class InMemoryMetastore(MetastoreABC):
    def __init__(self, **conf):
        self.conf = conf
        self._hwms: List[HWM] = []

    @property
    def hwms(self):
        return self._hwms

    @property
    def hwm(self) -> Optional[HWM]:
        return next((h for h in reversed(self._hwms)), None)

    @hwm.setter
    def hwm(self, value: HWM):
        if not isinstance(value, HWM):
            raise ValueError('The value should be a HWM.')
        self._hwms.append(value)

    def set_hwm(self, key, literal_value) -> HWM:
        hwm = HWM.from_literal(key, literal_value)
        self.hwm = hwm
        return hwm

    def get_hwm(self, key) -> Optional[HWM]:
        return next((h for h in reversed(self._hwms) if h.expression == key), None)

    def get_hwms(self, key, count=1) -> List[HWM]:
        hwms = []
        for hwm in reversed(self._hwms):
            if not count:
                break

            if hwm.expression == key:
                count -= 1
                hwms.append(hwm)

        return list(reversed(hwms))


def metastore_factory(metastore_spec: MutableMapping) -> MetastoreABC:
    metastore_type = metastore_spec['type']
    if metastore_type == 'in_memory':
        metastore = InMemoryMetastore()
    elif metastore_type == 'hdfs':
        raise NotImplementedError('HDFSMetastore is not implemented yet.')
    elif metastore_type == 'redis':
        raise NotImplementedError('RedisMetastore is not implemented yet.')
    else:
        raise ValueError(f'Unknown metastore type: "{metastore_type}".')
    return metastore
