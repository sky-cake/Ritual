from abc import ABC, abstractmethod


class DotDict(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


class BaseDb(ABC):
    placeholder: str

    @abstractmethod
    def get_upsert_clause(self, conflict_col: str, update_cols: list[str]) -> str:
        pass

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def save_and_close(self):
        pass

    @abstractmethod
    def run_query_tuple(self, sql_string: str, params: tuple=None, commit: bool=False):
        pass

    @abstractmethod
    def run_query_dict(self, sql_string: str, params: tuple=None, commit: bool=False):
        pass

    @abstractmethod
    def run_query_many(self, sql_string: str, params: tuple=None, commit: bool=False, dict_row=False):
        pass
