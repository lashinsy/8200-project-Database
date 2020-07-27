import shelve
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Any, Dict, List
import db_api
import os


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):

    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            index = 0
            for field in self.fields:
                if field.name != key_field_name:
                    db[field.name] = [index, field.type]
                    index += 1
            self.count_fields = len(db)

    def count(self) -> int:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), 'r') as db:
            count = len(db) - self.count_fields
        return count

    def insert_record(self, values) -> None:
        # check if get value for primary key
        if not values.get(self.key_field_name):
            raise ValueError("ppp---")
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            # check if value of primary key exists in the data
            if str(values[self.key_field_name]) in list(db.keys()):
                raise ValueError("ooo---")
            db[str(values[self.key_field_name])] = [None] * self.count_fields
            for key in values.keys():
                # if every key and value that I get is suitable to the data
                if key != self.key_field_name:
                    if not db.get(key) or not isinstance(values[key], db[key][1]):
                        self.delete_record(values[self.key_field_name])
                        raise ValueError("llll----")
                    db[str(values[self.key_field_name])].insert(db[key][0], values[key])

    def delete_record(self, key: Any) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            if key in db.keys():
                del db[key]

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[db_api.SelectionCriteria]) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    def __init__(self):
        self.tables = dict()

    # להוסיף בדיקות האם הטבלה קיימת בכלל
    def create_table(self, table_name, fields, key_field_name) -> DBTable:
        self.tables[table_name] = DBTable(table_name, fields, key_field_name)
        return self.tables[table_name]

    def num_tables(self) -> int:
        return len(self.tables)

    def get_table(self, table_name) -> DBTable:
        return self.tables.get(table_name)

    # check if shelve create more than one file
    def delete_table(self, table_name) -> None:
        os.remove(db_api.DB_ROOT.joinpath(f"{table_name}"))
        self.tables.pop(table_name)

    def get_tables_names(self) -> List[Any]:
        return list(self.tables.keys())

    def query_multiple_tables(self, tables, fields_and_values_list, fields_to_join_by) -> List[Dict[str, Any]]:
        pass
