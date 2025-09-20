import sqlite3
from sqlite3.dbapi2 import DataError
import threading
from typing import Any, Union, Optional

import random
import string

from .datatypes import *
from .errors import *

# TODO: in-code documentation

def _serial():
    serial = ""
    while len(serial) != 8:
        serial += random.choice(string.hexdigits)
    return serial

class ExecutionObject:

    def __init__(self,
        path: str,
        separate_thread: bool = False,
        await_completion: bool = True
    ):
        super(ExecutionObject, self).__init__()
        if not isinstance(self, DatabaseObject):
            raise InstanceError("instance is not a DatabaseObject")

        if not path.endswith(".db"):
            path += ".db"
        self.name = path.replace("\\", "/").split("/")[-1][:-3]

        self.separate_thread = separate_thread
        self.await_completion = await_completion

        self._toRead = list()
        self._toWrite = list()
        self._awaited = list()
        self._results = dict()

        self.alive = True
        if self.separate_thread:
            threading.Thread(target=self._executions, daemon=True).start()
        self.connection = sqlite3.connect(path, check_same_thread=False)

    def waitForQueue(self):
        """ Waits until the queue is empty. """
        while self.alive:
            if not self._toRead and not self._toWrite:
                return
        raise DatabaseError("cannot wait for queue in closed database")

    def _read(self, obj: 'ReadObject') -> Optional[list[Any]]:
        if self.separate_thread:
            self._toRead.append(obj)
            self._awaitCompletion(obj, False)
        else:
            self._execute(obj)
        return self._getResults(obj)

    def _write(self, obj: 'WriteObject'):
        if self.separate_thread:
            self._toWrite.append(obj)
            self._awaitCompletion(obj, False)
        else:
            self._execute(obj)

    def _awaitCompletion(self, obj: 'QueryObject', preference: bool):
        await_completion = preference
        if obj.await_completion is not None:
            await_completion = obj.await_completion
        elif self.await_completion is not None:
            await_completion = self.await_completion
        if not await_completion:
            return
        while self.alive:
            if obj.serial in self._awaited:
                index = self._awaited.index(obj.serial)
                self._awaited.pop(index)
                return
        raise DatabaseError("cannot await completion from closed database")

    def _getResults(self, obj: 'ReadObject') -> Optional[list[Any]]:
        while self.alive:
            if obj.serial in self._results:
                result = self._results[obj.serial]
                del self._results[obj.serial]
                return result
        raise DatabaseError("cannot retrieve data from closed database")

    def _simplify(self,
        obj: 'GetObject',
        result: Optional[list[Any]]
    ) -> bool:
        if len(obj.items) != 1:
            return False
        if obj.items[0] == "*":
            return False
        if result is None:
            return False
        return True

    def _execute(self, obj: 'QueryObject'):
        if not isinstance(obj, QueryObject):
            raise InstanceError("instance is not a valid QueryObject")

        failed = True
        error_message = ""
        cursor = self.connection.cursor()
        try:
            cursor.execute(obj._query(), tuple(obj.inputs))
            failed = False
        except sqlite3.OperationalError as exception:
            error_message = exception.args[0]
        except sqlite3.IntegrityError as exception:
            error_message = exception.args[0]

        if failed:
            inputs = list()
            for input in obj.inputs:
                if not isinstance(input, (str, blob)):
                    inputs.append(str(input))
                    continue
                inputs.append("'" + str(input) + "'")
            if isinstance(obj, (GetObject, RawReadObject)):
                self._results[obj.serial] = None
            cursor.close()
            if obj.await_completion or (obj.await_completion is None and self.await_completion):
                self._awaited.append(obj.serial)
            raise QueryError(error_message, obj.query, inputs)

        if isinstance(obj, RawReadObject):
            result = cursor.fetchall()
            self._results[obj.serial] = result

        if isinstance(obj, GetObject):
            if obj.get_type == "first":
                result = cursor.fetchone()
                if self._simplify(obj, result):
                    result = result[0]
            elif obj.get_type == "all":
                result = cursor.fetchall()
                if len(obj.items) == 1 and obj.items[0] != "*":
                    # Potentially yield items back
                    result = [items[0] for items in result]
            else:
                raise LogicError("invalid get type")
            self._results[obj.serial] = result

        cursor.close()

        if isinstance(obj, WriteObject):
            self.connection.commit()

        if obj.await_completion or (obj.await_completion is None and self.await_completion):
            self._awaited.append(obj.serial)

    def _executions(self):
        while self.alive:
            if self._toRead:
                obj = self._toRead[0]
                self._execute(obj)
                index = self._toRead.index(obj)
                self._toRead.pop(index)
            elif self._toWrite:
                obj = self._toWrite[0]
                self._execute(obj)
                index = self._toWrite.index(obj)
                self._toWrite.pop(index)

# TODO: pragma values
class DatabaseObject(ExecutionObject):
    """ A database object which allows interaction with an SQLite database.

        Parameters
         - path: The path to the database file.
         - separate_thread: Whether the database should use a separate thread for execution.
         - await_completion: Whether the database should wait for queries to complete before returning.

         NOTE: If separate_thread is True, await_completion will be ignored.
    """

    def __init__(self,
        path: str,
        separate_thread: bool = False,
        await_completion: bool = True
    ):
        super(DatabaseObject, self).__init__(
            path,
            separate_thread,
            await_completion
        )

    def table(self, name: str) -> 'TableObject':
        """ Returns a table object.
                Parameters
                - name: The name of the table.
            """
        table = TableObject(self, name)
        if not table.exists:
            raise TableError("table does not exist")
        return table

    def create(self,
        table_name: str,
        columns: Optional[dict[str, Any]] = None,
        await_completion: bool = True,
        must_not_exist: bool = False,
        **kwargs: Any
    ) -> 'TableObject':
        """ Creates a table within the database.

            Parameters
             - table_name: The name of the table.
             - columns: A dictionary containing the columns and their types.
             - await_completion: Whether to wait for the table to be created before returning.
             - must_not_exist: Whether to raise an error if the table already exists.
             - kwargs: Alternatively, the columns and their types can be passed as keyword arguments.

             NOTE: If both columns and kwargs are passed, columns will be used.
             NOTE: kwargs cannot share the same name with a named arg or kwarg.
        """

        if kwargs is None and columns is None:
            raise InputError("you must provide either columns or kwargs")
        elif columns is not None:
            kwargs = columns

        if must_not_exist:
            if TableObject(self, table_name).exists:
                raise TableError("table already exists")

        return CreateTableObject(self, table_name, kwargs).run(await_completion)

    def drop(self, table: str):
        """ Drops a table. """
        query = f"DROP TABLE {table}"
        RawWriteObject(query, database=self).run()
        del self

    def exists(self, table: str) -> bool:
        """ Returns whether the table exists. """
        self.waitForQueue()
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        return bool(RawReadObject(query, database=self).run())

    @property
    def tables(self) -> list[str]:
        """ Returns a list of all tables in the database. """
        self.waitForQueue()
        query = f"SELECT name FROM sqlite_master WHERE type='table'"
        tables = list()
        res = RawReadObject(query, database=self).run()
        if not res:
            return tables
        for item in res:
            tables.append(item[0])
        return tables

    @property
    def queue(self) -> int:
        """ Returns the number of queries in the queue. """
        return len(self._toRead) + len(self._toWrite)

    def close(self, ignore_queue: bool = False):
        """ Closes the connection to the database. """
        if not self.alive:
            raise DataError("DatabaseObject is already closed")
        if not ignore_queue:
            self.waitForQueue()
        self.alive = False
        self._toRead = list()
        self._toWrite = list()
        self.connection.close()

    # TODO: optimse database
    def optimise(self):
        raise NotImplemented("database optimising has not yet been implemented")

    def __enter__(self) -> 'DatabaseObject':
        return self

    def __exit__(self, *args: Any):
        self.close()

    def __repr__(self) -> str:
        return f"<{self.name} database object: {len(self.tables)} tables>"
    __str__ = __repr__

class TableObject:
    """ A table object is used to interact with a table within a database.

        Parameters
         - database: The database object to use.
         - table: The name of the table.
    """

    def __init__(self, database: DatabaseObject, table: str):
        super(TableObject, self).__init__()
        if not isinstance(database, DatabaseObject):
            raise InstanceError("instance is not a DatabaseObject")

        self.database = database
        self.name = table

    def __enter__(self):
        return self

    def __exit__(self, *args: Any):
        self.database.close()

    def rename(self, name: str):
        """ Renames the table.

            Parameters
             - name: The new name of the table.
        """
        if name in self.database.tables:
            raise TableError("table already exists")
        query = f"ALTER TABLE {self.name} RENAME TO '{name}'"
        RawWriteObject(query, table=self).run()
        self.name = name

    def drop(self):
        """ Drops the table. """
        query = f"DROP TABLE {self.name}"
        RawWriteObject(query, table=self).run()
        del self

    def addColumn(self,
        values: Optional[dict[str, Any]] = None,
        **kwargs: Any
    ):
        """ Adds a column to the table.

            Parameters
             - values: A dictionary containing the column name and its types.
             - kwargs: Alternatively, the column and its type can be passed as a keyword argument.
        """
        # TODO: refitting table
        if kwargs is None and values is None:
            raise InputError("you must provide either values or kwargs")
        elif values is not None:
            kwargs = values
        AddColumnObject(self, kwargs, refit=False, **kwargs).run()

    # TODO: remove column
    def removeColumn(self):
        """ Removes a column from the table.

            Note: This method is not yet implemented.
        """
        raise NotImplemented("removing columns has not yet been implemented")

    def add(self,
        values: Optional[dict[str, Any]] = None,
        **kwargs: Any
    ):
        """ Adds a row to the table.

            Parameters
                - values: A dictionary of columns and the values to add.
        """
        AddRowObject(self, values, **kwargs).run()

    def remove(self) -> 'RemoveRowObject':
        """ Starts a query to remove a row from the table. """
        return RemoveRowObject(self)

    def get(self, *items: str) -> 'GetObject':
        """ Starts a query to get the first result from the table.

            Parameters
             - items: The columns to return. If none are specified, all columns will be returned.
        """
        return GetObject(self, "first", *items)
    getFirst = get

    def getAll(self, *items: str) -> 'GetObject':
        """ Starts a query to get all results from the table.

            Parameters
             - items: The columns to return. If none are specified, all columns will be returned.
        """
        return GetObject(self, "all", *items)

    def set(self,
        *values: Optional[dict[str, Any]],
        **kwargs: Any
    ) -> 'SetObject':
        """ Starts a query to set a value in the table.

            Parameters
             - values: A dictionary of columns and the values to set.
        """
        return SetObject(self, *values, **kwargs)

    @property
    def exists(self) -> bool:
        """ Returns whether the table exists. """
        return self.database.exists(self.name)

    @property
    def columns(self) -> list[str]:
        """ Returns a list of all columns in the table. """
        self.database.waitForQueue()
        query = f"SELECT name FROM PRAGMA_TABLE_INFO('{self.name}')"
        columns = list()
        res = RawReadObject(query, table=self).run()
        if not res:
            return columns
        for item in res:
            columns.append(item[0])
        return columns

    @property
    def column_types(self) -> dict[str, str]:
        """ Returns a list of all columns and their types in the table. """
        self.database.waitForQueue()
        query = f"SELECT name, type FROM PRAGMA_TABLE_INFO('{self.name}')"
        columns = dict()
        res = RawReadObject(query, table=self).run()
        if not res:
            return columns
        for item in res:
            columns[item[0]] = item[1]
        return columns

    @property
    def rows(self) -> int:
        """ Returns the number of rows in the table. """
        self.database.waitForQueue()
        query = f"SELECT COUNT(*) AS count FROM {self.name}"
        res = RawReadObject(query, table=self).run()
        if not res:
            return 0
        return res[0][0]

    @property
    def primary_keys(self) -> list[str]:
        """ Returns a list of all primary keys in the table. """
        self.database.waitForQueue()
        query = f"SELECT name FROM PRAGMA_TABLE_INFO('{self.name}') WHERE pk=1"
        keys = list()
        res = RawReadObject(query, table=self).run()
        if not res:
            return keys
        for item in res:
            keys.append(item[0])
        return keys

    @property
    def foreign_keys(self) -> list[tuple[str, str, str]]:
        """ Returns a list of all foreign keys in the table. """
        self.database.waitForQueue()
        query = f"SELECT \"table\", \"from\", \"to\" FROM PRAGMA_FOREIGN_KEY_LIST('{self.name}')"
        items = list()
        res = RawReadObject(query, table=self).run()
        if not res:
            return items
        for item in res:
            items.append(item)
        return items

    def __repr__(self) -> str:
        columns = len(self.columns)
        return f"<{self.name} table object: {columns} columns, {self.rows} rows>"

class QueryObject:

    type: str
    items: tuple

    def __init__(self, database: DatabaseObject):
        super(QueryObject, self).__init__()
        if not isinstance(self, QueryObject):
            raise InstanceError("instance is not a valid QueryObject")
        self.database = database
        self.serial = _serial()
        self.inputs = list()

    def _run(self, await_completion: Optional[bool] = None):
        if not self.database.alive:
            raise DatabaseError("cannot run query in a closed database")
        self.await_completion = await_completion

    def run(self, await_completion: Optional[bool] = None) -> Any:
        raise NotImplemented("run method not implemented")

    async def asyncRun(self) -> Any:
        return self.run(await_completion=True)

    def _query(self) -> str:
        if not isinstance(self, QueryObject):
            raise InstanceError("instance is not a valid QueryObject")
        self.inputs = list()
        if hasattr(self, "serial"):
            return self.query()
        query = self.query()
        for input in self.inputs:
            query = query.replace("?", input, 1)
        return query

    def query(self) -> str:
        raise NotImplemented("query method not implemented")

    def __repr__(self):
        return f"<{self.type} query object>"

class LogicObject:

    def __init__(self,
        filter: 'FilterObject',
        item: str,
        conjunctive: Optional[str] = None
    ):
        super(LogicObject, self).__init__()
        if not isinstance(filter, FilterObject):
            raise InstanceError("instance is not a valid LogicObject")

        self.filter = filter.__copy__()
        self.item = item
        self.conjunctive = conjunctive

    def __copy__(self) -> 'LogicObject':
        return LogicObject(self.filter, self.item, self.conjunctive)

    def _logic(self) -> str:
        logic = ""
        inputs = list()
        if self.conjunctive:
            logic += " " + self.conjunctive
        logic += f" {self.item} " + self.operation
        if self.operation in ["IN", "NOT IN"]:
            for value in self.value:
                inputs.append(value)
            logic += " (" + ", ".join("?" * len(self.value)) + ")"
        else:
            logic += " ?"
            inputs.append(self.value)
        return inputs, logic

    def eq(self, value: Union[str, int, float, blob, None]) -> 'FilterObject':
        """ Checks if the value is equal to the specified value. """
        return self.add("=", value)
    equalto = equal = eq

    def neq(self, value: Union[str, int, float, blob, None]) -> 'FilterObject':
        """ Checks if the value is not equal to the specified value. """
        return self.add("!=", value)
    notequalto = notequal = neq

    def gt(self, value: Union[int, float]) -> 'FilterObject':
        """ Checks if the value is greater than the specified value. """
        assert isNumber(value)
        return self.add(">", value)
    greaterthan = gt

    def lt(self, value: Union[int, float]) -> 'FilterObject':
        """ Checks if the value is less than the specified value. """
        assert isNumber(value)
        return self.add("<", value)
    lessthan = lt

    def gteq(self, value: Union[int, float]) -> 'FilterObject':
        """ Checks if the value is greater than or equal to the specified value. """
        assert isNumber(value)
        return self.add(">=", value)
    greaterthanorequalto = gteq

    def lteq(self, value: Union[int, float]) -> 'FilterObject':
        """ Checks if the value is less than or equal to the specified value. """
        assert isNumber(value)
        return self.add("<=", value)
    lessthanorequalto = lteq

    def like(self, value: Union[str, blob]) -> 'FilterObject':
        """ Checks if the value is similar to the specified value. """
        assert isString(value)
        return self.add("LIKE", value)

    def nlike(self, value: Union[str, blob]) -> 'FilterObject':
        """ Checks if the value is not similar to the specified value. """
        assert isString(value)
        return self.add("NOT LIKE", value)
    notlike = nlike

    def contains(self, value: Union[str, blob]) -> 'FilterObject':
        """ Checks if the value contains the specified value. """
        assert isString(value)
        return self.add("LIKE", "%" + value + "%")

    def ncontains(self, value: Union[str, blob]) -> 'FilterObject':
        """ Checks if the value does not contain the specified value. """
        assert isString(value)
        return self.add("NOT LIKE", "%" + value + "%")
    notcontains = ncontains

    def startswith(self, value: Union[str, blob]) -> 'FilterObject':
        """ Checks if the value starts with the specified value. """
        assert isString(value)
        return self.add("LIKE", value + "%")

    def nstartswith(self, value: Union[str, blob]) -> 'FilterObject':
        """ Checks if the value does not start with the specified value. """
        assert isString(value)
        return self.add("NOT LIKE", value + "%")
    notstartswith = nstartswith

    def endswith(self, value: Union[str, blob]) -> 'FilterObject':
        """ Checks if the value ends with the specified value. """
        assert isString(value)
        return self.add("LIKE", "%" + value)

    def nendswith(self, value: Union[str, blob]) -> 'FilterObject':
        """ Checks if the value does not end with the specified value. """
        assert isString(value)
        return self.add("NOT LIKE", "%" + value)
    notendswith = nendswith

    def null(self) -> 'FilterObject':
        """ Checks if the value is null. """
        return self.add("IS", None)
    isnull = null

    def notnull(self) -> 'FilterObject':
        """ Checks if the value is not null. """
        return self.add("IS NOT", None)
    isnotnull = notnull

    def IN(self, *values: Union[str, int, float, blob, None]) -> 'FilterObject':
        """ Checks if the value is in the specified values. """
        return self.add("IN", values)

    def NIN(self, *values: Union[str, int, float, blob, None]) -> 'FilterObject':
        """ Checks if the value is not in the specified values. """
        return self.add("NOT IN", values)
    NOTIN = NIN

    def add(self, operation: str, value: Any) -> 'FilterObject':
        logic = self.__copy__()
        logic.operation = operation
        logic.value = value
        logic.filter.filters.append(logic)
        return logic.filter

    def __repr__(self) -> str:
        return f"<{self.filter.type} logic object>"

class FilterObject:

    type: str
    values: tuple
    inputs: list

    def __init__(self):
        super(FilterObject, self).__init__()
        if not isinstance(self, QueryObject):
            raise InstanceError("instance is not a QueryObject")

        self.filters = list()

    def _filter(self) -> str:
        if not self.filters:
            return ""
        query = " WHERE"
        for filter in self.filters:
            i, q = filter._logic()
            self.inputs += i
            query += q
        return query

    def where(self, item: str) -> 'LogicObject':
        """ Filters the query based on the specified item. """
        if not self.filters:
            return LogicObject(self, item)
        raise LogicError("already performing logic")

    def AND(self, item: str) -> LogicObject:
        """ Allows for multiple logic operations to be performed where all operations must be true. """
        if self.filters:
            return LogicObject(self, item, conjunctive="AND")
        raise LogicError("no item to perform logic on")

    def OR(self, item) -> LogicObject:
        """ Allows for multiple logic operations to be performed where only one of the operations must be true. """
        if self.filters:
            return LogicObject(self, item, conjunctive="OR")
        raise LogicError("no item to perform logic on")

class SortObject:

    def __init__(self):
        super(SortObject, self).__init__()
        if not isinstance(self, QueryObject):
            raise InstanceError("instance is not a valid QueryObject")

        self.order = "DESC"
        self.sorted = tuple()
        self.sortlimit: Optional[int] = None

    def _sort(self) -> str:
        if not self.sorted:
            return ""
        statement = " ORDER BY "
        statement += ", ".join(self.sorted)
        statement += " " + self.order
        if self.sortlimit is not None:
            statement += " LIMIT " + str(self.sortlimit)
        return statement

    def sort(self, *items: str) -> 'SortObject':
        """ Sort the records by the specified items.

            Parameters:
             - items: The items to sort by.
        """
        obj = self.__copy__()
        obj.sorted = items
        return obj

    def asc(self) -> 'SortObject':
        """ Sorts the items in ascending order. """
        if not self.sorted:
            raise SortError("no reference has been provided (use .sort first)")
        obj = self.__copy__()
        obj.order = "ASC"
        return obj

    def desc(self) -> 'SortObject':
        """ Sorts the items in descending order. """
        if not self.sorted:
            raise SortError("no reference has been provided (use .sort first)")
        obj = self.__copy__()
        obj.order = "DESC"
        return obj

    def limit(self, limit: int) -> 'SortObject':
        """ Limits the number of items returned.

            Parameters:
             - limit: The maximum number of items to return.
        """
        if not self.sorted:
            raise SortError("no reference has been provided (use .sort first)")
        obj = self.__copy__()
        obj.sortlimit = limit
        return obj

class ReadObject(QueryObject):
    pass

class WriteObject(QueryObject):
    pass

class RawReadObject(ReadObject):
    """ Allows for raw read queries to be performed.

        Parameters:
         - rawquery: The raw query to perform.
         - table: The table of the database to perform the query on.
         - database: The database to perform the query on.
    """

    type = "raw read"

    def __init__(self,
        rawquery: str,
        table: Optional[TableObject] = None,
        database: Optional[DatabaseObject] = None
    ):
        if table is not None:
            if not isinstance(table, TableObject):
                raise InstanceError("instance is not a TableObject")
            database = table.database
        elif database is not None:
            if not isinstance(database, DatabaseObject):
                raise InstanceError("instance is not a DatabaseObject")
        if database is None:
            raise InputError("you must provide either a table or a database")
        super(RawReadObject, self).__init__(database)
        self.rawquery = rawquery

    def run(self, await_completion: Optional[bool] = None) -> Optional[list[Any]]:
        """ Runs the query to create the table.

            Parameters
             - await_completion: Whether to wait for the table to be created before returning.
        """
        self._run(await_completion)
        return self.database._read(self)

    def query(self) -> str:
        return self.rawquery

class RawWriteObject(WriteObject):
    """ Allows for raw write queries to be performed.

        Parameters:
         - rawquery: The raw query to perform.
         - table: The table of the database to perform the query on.
         - database: The database to perform the query on.
    """

    type = "raw write"

    def __init__(self, rawquery, table=None, database=None):
        if table is not None:
            if not isinstance(table, TableObject):
                raise InstanceError("instance is not a TableObject")
            database = table.database
        elif database is not None:
            if not isinstance(database, DatabaseObject):
                raise InstanceError("instance is not a DatabaseObject")
        if database is None:
            raise InputError("you must provide either a table or a database")
        super(RawWriteObject, self).__init__(database)
        self.rawquery = rawquery

    def run(self, await_completion: Optional[bool] = None):
        """ Runs the query to create the table.

            Parameters
             - await_completion: Whether to wait for the table to be created before returning.
        """
        self._run(await_completion)
        self.database._write(self)

    def query(self) -> str:
        return self.rawquery

class CreateTableObject(WriteObject):

    type = "create table"

    def __init__(self,
        database: DatabaseObject,
        name: str,
        columns: dict[str, str],
        **kwargs: Any
    ):
        if not isinstance(database, DatabaseObject):
            raise InstanceError("instance is not a DatabaseObject")
        if not (columns or kwargs):
            raise InputError("you must provide columns for the table")

        super(CreateTableObject, self).__init__(database)

        if kwargs:
            columns = kwargs

        values = dict()
        for item in columns:
            value = columns[item]
            if isinstance(value, primary):
                if value.autoincrement and value.type is not int:
                    raise TypeError("primary keys with autoincrementation must be integers")
                elif value.type not in [str, blob, int, float]:
                    raise TypeError(f"'{value.type}' is an invalid data type")
                values[item] = value
            elif isinstance(value, foreign):
                if isinstance(value.table, str):
                    if value.table not in self.database.tables:
                        raise InstanceError(f"table '{value.table}' does not exist")
                    value.table = self.database.table(value.table)
                elif isinstance(value.table, TableObject):
                    if value.table.database != self.database:
                        raise InstanceError("foreign key table is from a different database")
                else:
                    raise TypeError("foreign key table must be a string or TableObject")
                if value.column is None:
                    keys = value.table.primary_keys
                    if len(keys) == 0:
                        raise InstanceError("referenced table has no primary key")
                    if len(keys) > 1:
                        raise InstanceError("referenced table has multiple primary keys; you must specify which one to reference")
                    value.column = keys[0]
                elif value.column in value.table.columns:
                    value.column = value.column
                else:
                    raise InstanceError(f"column '{value.column}' does not exist in referenced table '{table.name}'")
                value.type = value.table.column_types[value.column]
                values[item] = value
            elif isinstance(value, (unique, default, null, notnull)):
                if value.type not in [str, blob, int, float]:
                    raise TypeError(f"'{value.type}' is an invalid data type")
                values[item] = value
            elif value is primary:
                values[item] = primary()
            elif value in [str, blob, int, float]:
                values[item] = null(value)
            else:
                raise TypeError(f"'{value}' is an invalid data type")

        self.name = name
        self.values = values

    def run(self, await_completion: Optional[bool] = None) -> 'TableObject':
        """ Runs the query to create the table.

            Parameters
             - await_completion: Whether to wait for the table to be created before returning.
        """
        self._run(await_completion)
        self.database._write(self)
        return TableObject(self.database, self.name)

    def query(self) -> str:
        query = f"CREATE TABLE IF NOT EXISTS {self.name} ("
        lines = list()
        autoincrement = False
        primaries = list()
        for item in self.values:
            line = item + " "
            value = self.values[item]
            if value.type is str:
                line += "TEXT"
            elif value.type is int:
                line += "INTEGER"
            elif value.type is float:
                line += "REAL"
            elif value.type is blob:
                line += "BLOB"
            if isinstance(value, primary):
                if value.autoincrement:
                    autoincrement = True
                    line += " NOT NULL PRIMARY KEY AUTOINCREMENT"
                else:
                    line += " NOT NULL"
                primaries.append(item)
            elif isinstance(value, foreign):
                assert isinstance(value.table, TableObject)
                assert isinstance(value.table.name, str)
                assert isinstance(value.column, str)
                line += " NOT NULL REFERENCES " + value.table.name + "(" + value.column + ")"
            elif isinstance(value, default):
                line += " DEFAULT "
                if value.type in [str, blob]:
                    line += "'" + value.value + "'"
                elif value.type in [int, float]:
                    line += str(value.value)
            elif isinstance(value, unique):
                line += " NOT NULL UNIQUE"
            elif isinstance(value, notnull):
                line += " NOT NULL"
            lines.append(line)
        if autoincrement and len(primaries) > 1:
            raise TypeError(
                "cannot autoincrement primary key with two or more primary keys."
                " Try using the 'unique' typing for the other keys"
            )
        elif not autoincrement and len(primaries) > 0:
            lines.append("PRIMARY KEY (" + ", ".join(primaries) + ")")
        return query + ", ".join(lines) + ")"

class AddColumnObject(WriteObject):

    type = "add column"

    def __init__(self,
        table: TableObject,
        values: dict[str, Any],
        refit: bool = False,
        **kwargs: Any
    ):
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")
        if not (values or kwargs):
            raise InputError("you must provide a column to be added")

        super(AddColumnObject, self).__init__(table.database)

        if kwargs:
            values = kwargs

        name = list(values.keys())[0]
        value = values[name]
        if isinstance(value, primary):
            if value.autoincrement and value.type is not int:
                raise TypeError("primary keys with autoincrementation must be integers")
            elif value.type not in [str, blob, int, float]:
                raise TypeError(f"'{value.type}' is an invalid data type")
        elif isinstance(value, (unique, default, null, notnull)):
            if value.type not in [str, blob, int, float]:
                raise TypeError(f"'{value.type}' is an invalid data type")
        elif value in [primary, str, blob, int, float]:
            value = null(value)
        else:
            raise TypeError(f"'{value}' is an invalid data type")

        if name in table.columns:
            raise TableError("column already exists")

        self.table = table
        self.refit = refit
        self.name = name
        self.value = value

    def run(self, await_completion: Optional[bool] = None):
        """ Runs the query to create the table.

            Parameters
             - await_completion: Whether to wait for the table to be created before returning.
        """
        self._run(await_completion)
        self.database._write(self)

    def query(self) -> str:
        if not self.refit:
            query = f"ALTER TABLE {self.table.name} "
            query += f"ADD COLUMN {self.name} "
            value = self.value
            if value.type is str:
                query += "TEXT"
            elif value.type is int:
                query += "INTEGER"
            elif value.type is float:
                query += "REAL"
            elif value.type is blob:
                query += "BLOB"
            elif value.type is primary:
                query += "INTEGER PRIMARY KEY"
            if isinstance(value, primary):
                if value.autoincrement:
                    raise TypeError("cannot add an autoincrement value to a new column")
                query += " PRIMARY KEY"
            elif isinstance(value, default):
                query += " DEFAULT "
                if value.type in [str, blob]:
                    query += "'" + value.value + "'"
                elif value.type in [int, float]:
                    query += str(value.value)
            elif isinstance(value, unique):
                raise TypeError("cannot add a unique value to a new column")
            elif isinstance(value, notnull):
                raise TypeError("cannot add a not null value to a new column")
            return query
        raise NotImplemented("refitting tables has not yet been implemented")

class AddRowObject(WriteObject):

    type = "add row"

    def __init__(self, table, values, **kwargs):
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")
        if not (values or kwargs):
            raise InputError("you must provide values to be added")

        super(AddRowObject, self).__init__(table.database)

        if kwargs:
            values = kwargs

        self.table = table
        self.values = values.copy()

    def run(self, await_completion: Optional[bool] = None):
        """ Runs the query to create the table.

            Parameters
             - await_completion: Whether to wait for the table to be created before returning.
        """
        self._run(await_completion)
        self.database._write(self)

    def query(self) -> str:
        query = f"INSERT INTO {self.table.name} ({', '.join(self.values)}) "
        query += f"VALUES ({', '.join('?' * len(self.values))})"
        self.inputs = list(self.values.values())
        return query

class RemoveRowObject(WriteObject, FilterObject):

    type = "remove row"

    def __init__(self, table):
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")
        super(RemoveRowObject, self).__init__(table.database)

        self.table = table

    def run(self, await_completion: Optional[bool] = None):
        """ Runs the query to create the table.

            Parameters
             - await_completion: Whether to wait for the table to be created before returning.
        """
        self._run(await_completion)
        self.database._write(self)

    def query(self) -> str:
        query = f"DELETE FROM {self.table.name}"
        return query + self._filter()

class JoinObject(QueryObject):

    def __init__(self, table: TableObject):
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")
        super(JoinObject, self).__init__(table.database)
        self.table = table
        self.joins = list()

    def __copy__(self) -> 'JoinObject':
        obj = JoinObject(self.table)
        obj.joins = self.joins.copy()
        return obj

    def join(self,
        type: str,
        left: Optional[str] = None,
        right: Optional[str] = None
    ):
        if not left:
            keys = self.table.foreign_keys
            if len(keys) == 0:
                raise DatabaseError("left table has no foreign key")
            if len(keys) > 1:
                raise DatabaseError("left table has multiple foreign keys; you must specify which one to reference")
            table, left, right = keys[0]
        elif left not in self.table.columns:
            raise DatabaseError(f"column '{left}' does not exist in left table '{self.table.name}'")
        elif not right:
            keys = self.table.foreign_keys
            found = False
            for key in keys:
                if key[1] == left:
                    found = True
                    break
            if not found:
                raise DatabaseError(f"column '{left}' is not a foreign key")
            table, left, right = keys[0]
        elif "." not in right:
            raise DatabaseError(f"you must specify the table for the right column eg: 'table.{right}'")
        else:
            table, right = right.split(".", 1)
            if table not in self.database.tables:
                raise DatabaseError(f"table '{table}' does not exist")
            if right not in self.database.table(table).columns:
                raise DatabaseError(f"column '{right}' does not exist in right table '{table.name}'")
        self.joins.append((type.upper(), table, left, right))

    def query(self) -> str:
        if not self.joins:
            return ""
        query = ""
        for join in self.joins:
            type, table, left, right = join
            if type not in ["INNER", "LEFT", "RIGHT", "FULL"]:
                raise TypeError(f"'{type}' is not a valid join type")
            query += f" {type} JOIN {table} ON {self.table.name}.{left}={table}.{right}"
        return query

class GetObject(ReadObject, FilterObject, SortObject):

    type = "get row"

    def __init__(self,
        table: TableObject,
        get_type: str,
        *items: str
    ):
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")
        super(GetObject, self).__init__(table.database)

        if get_type not in ["first", "all"]:
            raise TypeError("invalid get type")

        if not items:
            items = ("*",)

        self.table = table
        self.get_type = get_type
        self.items = items
        self.joins = JoinObject(self.table)

    def __copy__(self) -> 'GetObject':
        obj = GetObject(self.table, self.get_type, *self.items)
        obj.joins = self.joins.__copy__()
        obj.order = getattr(self, "order", "DESC")
        obj.sorted = getattr(self, "sorted", tuple())
        obj.sortlimit = getattr(self, "sortlimit", None)
        obj.filters = self.filters.copy()
        return obj

    def join(self,
        left: Optional[str] = None,
        right: Optional[str] = None,
        type: str = "LEFT"
    ) -> 'GetObject':
        """ Joins another table to the query.

            Parameters
             - left: The current (left) table's column to join. If one isn't specified, the foreign key of the table will be used.
             - right: The reference (right) table's column to join. If one isn't specified, the left column's linked key will be used.
             - type: The type of join to perform. Defaults to a left join.
        """
        obj = self.__copy__()
        obj.joins.join(type, left, right)
        return obj

    def ljoin(self,
        left: Optional[str] = None,
        right: Optional[str] = None
    ) -> 'GetObject':
        """ Joins another table to the query. Defaults to a left join.

            Parameters
             - left: The current (left) table's column to join. If one isn't specified, the foreign key of the table will be used.
             - right: The reference (right) table's column to join. If one isn't specified, the left column's linked key will be used.

        """
        return self.join(table, left, right, type="LEFT")
    leftjoin = ljoin

    def rjoin(self,
        left: Optional[str] = None,
        right: Optional[str] = None
    ) -> 'GetObject':
        """ Joins another table to the query.

            Parameters
             - left: The current (left) table's column to join. If one isn't specified, the foreign key of the table will be used.
             - right: The reference (right) table's column to join. If one isn't specified, the left column's linked key will be used.
        """
        return self.join(left, right, type="RIGHT")
    rightjoin = rjoin

    def ijoin(self,
        left: Optional[str] = None,
        right: Optional[str] = None
    ) -> 'GetObject':
        """ Joins another table to the query.

            Parameters
             - left: The current (left) table's column to join. If one isn't specified, the foreign key of the table will be used.
             - right: The reference (right) table's column to join. If one isn't specified, the left column's linked key will be used.
        """
        return self.join(left, right, type="INNER")
    innerjoin = ijoin

    def fjoin(self,
        left: Optional[str] = None,
        right: Optional[str] = None
    ) -> 'GetObject':
        """ Joins another table to the query.

            Parameters
             - left: The current (left) table's column to join. If one isn't specified, the foreign key of the table will be used.
             - right: The reference (right) table's column to join. If one isn't specified, the left column's linked key will be used.
        """
        return self.join(left, right, type="FULL")
    fulljoin = fjoin

    def run(self, await_completion: Optional[bool] = None) -> Optional[list[Any]]:
        """ Runs the query to create the table.

            Parameters
             - await_completion: Whether to wait for the table to be created before returning.
        """
        self._run(await_completion)
        return self.database._read(self)

    def query(self) -> str:
        query = f"SELECT {', '.join(self.items)} FROM {self.table.name}"
        query += self.joins.query()
        return query + self._filter() + self._sort()

class SetObject(WriteObject, FilterObject, SortObject):

    type = "set row"

    def __init__(self,
        table: TableObject,
        values: dict[str, Any] | None = None,
        **kwargs: Any
    ):
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")
        super(SetObject, self).__init__(table.database)

        if kwargs:
            values = kwargs
        elif values is None:
            raise InputError("you must provide values to be set")

        self.table = table
        self.values = values

    def __copy__(self) -> 'SetObject':
        obj = SetObject(self.table, self.values)
        obj.order = getattr(self, "order", "DESC")
        obj.sorted = getattr(self, "sorted", tuple())
        obj.sortlimit = getattr(self, "sortlimit", None)
        obj.filters = self.filters.copy()
        return obj

    def run(self, await_completion: Optional[bool] = None):
        """ Runs the query to create the table.

            Parameters
             - await_completion: Whether to wait for the table to be created before returning.
        """
        self._run(await_completion)
        self.database._write(self)

    def query(self):
        query = f"UPDATE {self.table.name} SET "
        values = list()
        for item in self.values:
            value = self.values[item]
            if isinstance(value, (str, blob, int, float)):
                if isinstance(value, blob):
                    value = value.value
                values.append(f"{item}=?")
                self.inputs.append(value)
            elif isinstance(value, increment):
                values.append(f"{item}={item}+?")
                self.inputs.append(value.increment)
            elif isinstance(value, concatenate):
                values.append(f"{item}={item} || ?")
                self.inputs.append(value.concatenate)
            elif value is null or isinstance(value, null) or value is None:
                values.append(f"{item}=NULL")
            else:
                raise TypeError(f"'{type(value)}' is an invalid data type")
        return query + ", ".join(values) + self._filter() + self._sort()