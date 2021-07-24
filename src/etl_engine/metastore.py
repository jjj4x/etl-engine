from typing import List, Optional

from etl_engine.config import HWM


class InMemoryMetastore:
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
