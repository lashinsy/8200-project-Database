from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Any, Dict, List
import db_api
import os


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    # def __init__(self):


    def count(self) -> int:
        raise NotImplementedError

    def insert_record(self, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_record(self, key: Any) -> None:
        raise NotImplementedError

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
    tables: Dict[str, DBTable]

    def create_table(self, table_name, fields, key_field_name) -> DBTable:
        self.tables[table_name] = DBTable(table_name, fields, key_field_name)
        return self.tables[table_name]

    def num_tables(self) -> int:
        return len(self.tables)

    def get_table(self, table_name) -> DBTable:
        return self.tables.get(table_name)

# check if shelve create more than one file
    def delete_table(self, table_name) -> None:
        os.remove(db_api.DB_ROOT.joinpath(f"{table_name}.dir"))
        self.tables.pop(table_name)

    def get_tables_names(self) -> List[Any]:
        return list(self.tables.keys())

    def query_multiple_tables(self, tables, fields_and_values_list, fields_to_join_by) -> List[Dict[str, Any]]:
        pass

