from ast import TypeVar
from collections import deque
import collections.abc
import collections
from dataclasses import dataclass
import threading
import sqlite3
import typing
import time


class database_base[T= dict[str, typing.Any]]:
    INIT_STATEMENTS: str = ""

    def __init__(self, path: str = '.sqlite') -> None:
        self.database: sqlite3.Connection = sqlite3.connect(path)
        self.database.execute(self.INIT_STATEMENTS)
        self.database.execute(
            'create table if not exists CHECKED_IDS (id integer primary key)',
        )

    def add_to_data(self, iden: int, data: T | None) -> None:
        self.database.execute(
            f'insert or replace into CHECKED_IDS values ({iden})',
        )
        self.database.commit()

    def get_min(self) -> int | None:
        record = self.database.execute('''
            select id as I from CHECKED_IDS order by I asc limit 1
        ''').fetchone()
        return record[0] if record else None

    def get_max(self) -> int | None:
        record = self.database.execute(f'''
            select id as I from CHECKED_IDS order by I desc limit 1
        ''').fetchone()
        return record[0] if record else None

    def get_holes(self) -> list[tuple[int, int, int]]:
        return self.database.execute(f'''
            select prev_id, id, id - prev_id as diff from (
                select id, lag(id) over (order by id) as prev_id from checked_ids
            ) where id - prev_id > 1 order by diff asc
        ''').fetchall()

    def commit(self):
        self.database.commit()


T = TypeVar("T")


class SchemaValueType[T](typing.TypedDict):
    func: typing.Callable[[int, T], typing.Any]
    type: str


type SchemaType[T] = dict[str, dict[str, SchemaValueType[T]]]


class lambda_database[T = dict[str, typing.Any]](database_base[T]):
    # SCHEMA: SchemaType[T] = {}
    SCHEMA: SchemaType[typing.Any] = {}

    def __init__(self, path: str = '.sqlite') -> None:
        super().__init__(path)
        for table_name, table_fields in self.SCHEMA.items():
            params = ', '.join([
                f'"{field}" {item["type"]}'
                for i, (field, item) in enumerate(table_fields.items())
            ])
            _ = self.database.execute(
                f'create table if not exists "{table_name}" ({params})',
            )

    @staticmethod
    def __do_lambda(func: typing.Callable[[int, T | None], list[str]], iden: int, data: T | None) -> list[str]:
        try:
            return func(iden, data)
        except Exception as _:
            return []

    @typing.override
    def add_to_data(self, iden: int, data: T | None) -> None:
        super().add_to_data(iden, data)
        if data is None:
            return

        for table_name, table_fields in self.SCHEMA.items():
            fields_str = ', '.join(f'"{key}"' for key in table_fields.keys())
            fill_ins = ', '.join(['?'] * len(table_fields))
            vals = [
                self.__do_lambda(item['func'], iden, data)
                for _, item in table_fields.items()
            ]
            lens = [
                len(v)
                for v in vals
            ]
            max_len = max(lens)
            vals = [
                [None] * max_len
                if len == 0 else
                # Loops the array values around and truncates at `max_len`.
                (val * (max_len // len))[:max_len]
                for len, val in zip(lens, vals)
            ]
            params = list(zip(*vals))
            _ = self.database.executemany(
                f'insert or replace into "{table_name}" ({fields_str}) values ({fill_ins})', params,
            )
        self.database.commit()


class scraper_base[T= typing.Any]:

    RANGE_MIN: int = 1
    RANGE_MAX: int = 1 << 31
    DEFAULT_THREAD_COUNT: int = 8

    @staticmethod
    def try_entry(iden: int) -> T | None:
        raise NotImplementedError()

    @staticmethod
    def should_print_entry(iden: int, entry: T | None) -> bool:
        return entry is not None

    def __init__(
        self,
        database: database_base[T],
        iden_list: list[int],
        thread_count: int = 1,
    ) -> None:
        self.queue = deque[tuple[int, T | None]]()
        self.database = database
        self.thread_count = 0
        self.quit = False
        self.limit = 0

        def __thread_body(gen: collections.abc.Generator[tuple[int, T | None], None, None]) -> None:
            self.thread_count += 1
            for o in gen:
                self.queue.append(o)
            self.thread_count -= 1

        self.thread_list = [
            threading.Thread(
                target=__thread_body,
                args=[
                    self.__process(iden_list[o::thread_count]),
                ],
            )
            for o in range(0, thread_count)
        ]

    def is_in_range(self, iden: int, entry: T) -> bool:
        return self.RANGE_MIN <= iden <= self.RANGE_MAX

    def __print_progress(self, i: int, iden: int, entry: T | None) -> None:
        iden_str = f'{iden:10d}'
        if self.quit:
            print(f"{iden_str} ({i}/{self.limit} - {self.thread_count})\n", end='')
        elif self.should_print_entry(iden, entry):
            print(f"{iden_str}\n", end='')
        else:
            return

    def __process(self, r: collections.abc.Iterable[int]) -> collections.abc.Generator[tuple[int, T | None], None, None]:
        for i, base_iden in enumerate(r):
            if i > self.limit:
                if self.quit:
                    break
                self.limit = i

            entry = self.try_entry(base_iden)
            self.__print_progress(i, base_iden, entry)
            yield (base_iden, entry)

    def __join_threads(self) -> None:
        self.quit = True
        for t in self.thread_list:
            t.join()
            while len(self.queue) > 0:
                self.queue_pop()

    def queue_pop(self) -> None:
        i, e = self.queue.pop()
        self.database.add_to_data(i, e)

        if e and not self.is_in_range(i, e):
            self.__join_threads()
            return

    def run(self) -> None:
        for t in self.thread_list:
            t.start()

        try:
            while self.thread_count > 0:
                while len(self.queue) == 0:
                    time.sleep(0.1)
                self.queue_pop()

        except KeyboardInterrupt:
            print('Quitting program soon...')
            self.__join_threads()
