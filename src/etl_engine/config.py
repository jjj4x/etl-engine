from dataclasses import dataclass, field
from datetime import timedelta
from numbers import Number
from typing import Tuple, List, MutableMapping, Union

from etl_engine.metastore import HWM, MetastoreABC, InMemoryMetastore, metastore_factory


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
class Strategy:
    name: str = field(default='')
    increment: Union[None, Number, timedelta, str] = field(default=None)

    def __post_init__(self):
        if isinstance(self.increment, str):
            key, value = self.increment.split('=')
            self.increment = timedelta(**{key: value})


@dataclass
class ETL:
    source: Union[SourceTarget, MutableMapping] = field(default_factory=SourceTarget)
    target: Union[SourceTarget, MutableMapping] = field(default_factory=SourceTarget)
    strategy: Union[Strategy, MutableMapping] = field(default_factory=Strategy)

    def __post_init__(self):
        if not isinstance(self.source, SourceTarget):
            self.source = SourceTarget(**self.source)
        if not isinstance(self.target, SourceTarget):
            self.target = SourceTarget(**self.target)
        if not isinstance(self.strategy, Strategy):
            self.strategy = Strategy(**self.strategy)

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
    metastore: Union[MetastoreABC, MutableMapping] = field(default_factory=InMemoryMetastore)

    def __post_init__(self):
        if not isinstance(self.spark, Spark):
            self.spark = Spark(**self.spark)
        if not isinstance(self.etl, ETL):
            self.etl = ETL(**self.etl)
        if not isinstance(self.metastore, MetastoreABC):
            self.metastore = metastore_factory(self.metastore)

    def validate(self):
        self.etl.validate()
        return self

    @classmethod
    def from_yaml(cls):
        ...  # TODO

    @classmethod
    def from_mapping(cls, data: MutableMapping):
        return cls(**data)
