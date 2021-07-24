from dataclasses import dataclass, field
from datetime import date, datetime
from numbers import Number
from typing import Any, Tuple, List, MutableMapping, Union, Optional

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


@dataclass
class SourceTarget:
    schema: str = field(default='')
    name: str = field(default='')
    location: str = field(default='')
    system: str = field(default='')
    columns: List[str] = field(default_factory=list)
    where: str = field(default='')
    hwm: Union[HWM, MutableMapping] = field(default_factory=dict)

    def __post_init__(self):
        if self.hwm and not isinstance(self.hwm, HWM):
            self.hwm = HWM.from_mapping(**self.hwm)

    @property
    def fqdn(self):
        return f'{self.schema}.{self.name}'.strip('.')

    def validate(self):
        if not (self.fqdn or self.location):
            raise ValueError('Provide (schema and name) or location.')
        return self


@dataclass
class ETL:
    source: Union[SourceTarget, MutableMapping] = field(default_factory=SourceTarget)
    target: Union[SourceTarget, MutableMapping] = field(default_factory=SourceTarget)
    strategy: str = field(default='')

    def __post_init__(self):
        if not isinstance(self.source, SourceTarget):
            self.source = SourceTarget(**self.source)
        if not isinstance(self.target, SourceTarget):
            self.target = SourceTarget(**self.target)

    def validate(self):
        self.source.validate()
        self.target.validate()
        return self


@dataclass
class SparkWrite:
    mode: str = field(default=None)
    partitionBy: str = field(default=None)
    format: str = field(default=None)
    options: MutableMapping[str, str] = field(default_factory=dict)


@dataclass
class SparkRead:
    format: str = field(default=None)
    options: MutableMapping[str, str] = field(default_factory=dict)


@dataclass
class Spark:
    conf: List[Tuple[str, str]] = field(default_factory=list)
    read: Union[SparkRead, MutableMapping] = field(default_factory=dict)
    write: Union[SparkWrite, MutableMapping] = field(default_factory=dict)

    def __post_init__(self):
        if self.conf:
            self.conf = [tuple(k_v) for k_v in self.conf]
        if not isinstance(self.write, SparkWrite):
            self.write = SparkWrite(**self.write)
        if not isinstance(self.read, SparkRead):
            self.read = SparkRead(**self.read)


@dataclass
class ETLConf:
    spark: Union[Spark, MutableMapping] = field(default_factory=Spark)
    etl: Union[ETL, MutableMapping] = field(default_factory=ETL)

    def __post_init__(self):
        if not isinstance(self.spark, Spark):
            self.spark = Spark(**self.spark)
        if not isinstance(self.etl, ETL):
            self.etl = ETL(**self.etl)

    def validate(self):
        self.etl.validate()
        return self

    @classmethod
    def from_yaml(cls):
        ...  # TODO

    @classmethod
    def from_mapping(cls, data: MutableMapping):
        return cls(**data)
