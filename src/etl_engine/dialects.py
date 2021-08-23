from dataclasses import dataclass, field
from typing import List
from re import sub


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
        self.hint = self.escape(self.hint)
        self.columns = [self.escape(c) for c in self.columns]
        self.from_ = self.escape(self.from_)
        self.where = [self.escape(p) for p in self.where]
        self.other = self.escape(self.other)

    def __str__(self):
        where = ' and '.join(f'({s})' for s in self.where if s)
        statements = (
            'select',
            self.hint,
            ', '.join(self.columns) or '*',
            'from',
            self.from_,
            'where 1 = 1',
            f'and {where}' if where else '',
            self.other,
        )
        sql = ' '.join(s for s in statements if s).strip()
        sql = sub(r'(.*where 1 = 1)$', r'\1', sql)
        sql = sub(r'(.*where 1 = 1)\s?\(\)$', r'\1', sql)
        sql = sub(r'(.*where 1 = 1)\s?\(\)(.*)$', r'\1 \2', sql)
        return sql.strip()

    def __eq__(self, other):
        return str(self) == str(other)

    @staticmethod
    def escape(sql):
        return sub(r'(;.*)|(--[^\r\n]*)|(/\*[^+][\w\W]*?(?=\*)\*/)', '', sql)

    @property
    def sql(self):
        return str(self)
