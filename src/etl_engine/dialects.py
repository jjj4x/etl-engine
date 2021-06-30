from dataclasses import dataclass, field
from typing import List


@dataclass
class Query:
    hint: str = field(default='')
    columns: List[str] = field(default_factory=list)
    from_: str = field(default='')
    where: List[str] = field(default_factory=list)
    other: str = field(default='')

    def __post_init__(self):
        if not self.from_:
            raise ValueError('The FROM clause cannot be empty.')

    # TODO: injections (escaping)
    def __str__(self):
        statements = (
            'select',
            self.hint,
            ', '.join(self.columns) or '*',
            'from',
            self.from_,
            'where',
            ' and '.join(f'({s})' for s in self.where),
            self.other,
        )
        sql = ' '.join(s for s in statements if s).strip()
        if sql.endswith('where'):
            sql = sql.rstrip('where')
        if sql.endswith('where ()'):
            sql = sql.rstrip('where ()')
        return sql.strip()

    def __eq__(self, other):
        return str(self) == str(other)

    @property
    def sql(self):
        return str(self)
