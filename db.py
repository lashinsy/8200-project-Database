import shelve
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Any, Dict, List, Type
import db_api
import os

'''where we use operator[] on dict, we want to throw exception if the key doesn't exist'''


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

    def is_dict_suitable_table(self, table, dict_data):
        for key in dict_data.keys():
            if key != self.key_field_name:
                if not table.get(key) or not isinstance(dict_data[key], table[key][1]):
                    raise ValueError
            else:
                for field in self.fields:
                    if field.name == self.key_field_name:
                        if not isinstance(dict_data[key], field.type):
                            raise ValueError
        return True

    def count(self):
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), 'r') as db:
            count = len(db) - self.count_fields
        return count

    def insert_record(self, values):
        # check if get value for primary key
        if not values.get(self.key_field_name):
            raise ValueError
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            # check if value of primary key exists in the data
            if str(values[self.key_field_name]) in list(db.keys()):
                raise ValueError
            # if every key and value that I get is suitable to the data
            if self.is_dict_suitable_table(db, values):
                db[str(values[self.key_field_name])] = [None] * self.count_fields
                for key in values.keys():
                    if key != self.key_field_name:
                        db[str(values[self.key_field_name])].insert(db[key][0], values[key])

    def delete_record(self, key):
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            del db[key]

    def delete_records(self, criteria):
        list_to_delete = self.query_table(criteria)
        for item in list_to_delete:
            self.delete_record(item[self.key_field_name])

    def get_record(self, key):
        if not key:
            raise ValueError
        temp = dict()
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            if key not in db.keys():
                raise ValueError
            temp[self.key_field_name] = key
            for field in self.fields:
                if field != self.key_field_name:
                    temp[field] = db[key][db[field]]
        return temp

    def update_record(self, key, values):
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            if key not in db.keys():
                raise ValueError
            if self.is_dict_suitable_table(db, values):
                for val_key, val_value in values.items():
                    db[key][db[val_key]] = val_value

    def query_table(self, criteria):
        temp = list()
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            for key in db.keys():
                if key not in self.fields:
                    for item in criteria:
                        if eval(f'{db[key][db[item.field_name]]}{item.operator}{item.value}'):
                            temp.append(self.get_record(key))
                            break
        return temp

    def create_index(self, field_to_index):
        pass


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    def __init__(self):
        self.tables = dict()

    def create_table(self, table_name, fields, key_field_name):
        if table_name in self.tables.keys():
            raise ValueError
        self.tables[table_name] = DBTable(table_name, fields, key_field_name)
        return self.tables[table_name]

    def num_tables(self):
        return len(self.tables)

    def get_table(self, table_name):
        return self.tables[table_name]

    def delete_table(self, table_name):
        for suffix in ['bak', 'dat', 'dir']:
            os.remove(db_api.DB_ROOT.joinpath(f'{table_name}.{suffix}'))
        self.tables.pop(table_name)

    def get_tables_names(self):
        return list(self.tables.keys())

    def query_multiple_tables(self, tables, fields_and_values_list, fields_to_join_by):
        pass
