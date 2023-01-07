import sqlite3
from sqlite3.dbapi2 import DataError
import threading

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

class ExecuctionObject:

    _toRead = list()
    _toWrite = list()
    _awaited = list()
    _results = dict()

    def __init__(self):
        super(ExecuctionObject, self).__init__()
        if not isinstance(self, DatabaseObject):
            raise InstanceError("instance is not a DatabaseObject")

    def waitForQueue(self):
        """ Waits until the queue is empty. """
        while self.alive:
            if not self._toRead and not self._toWrite:
                return
        raise DatabaseError("cannot wait for queue in closed database")

    def _read(self, object):
        object.serial = _serial()
        self._toRead.append(object)
        self._awaitCompletion(object, False)
        return self._getResults(object)

    def _write(self, object):
        object.serial = _serial()
        self._toWrite.append(object)
        self._awaitCompletion(object, False)

    def _awaitCompletion(self, object, preference):
        await_completion = preference
        if object.await_completion is not None:
            await_completion = object.await_completion
        elif self.await_completion is not None:
            await_completion = self.await_completion
        if not await_completion:
            return
        while self.alive:
            if object.serial in self._awaited:
                index = self._awaited.index(object.serial)
                self._awaited.pop(index)
                return
        raise DatabaseError("cannot await completion from closed database")

    def _getResults(self, object):
        while self.alive:
            if object.serial in self._results:
                result = self._results[object.serial]
                del self._results[object.serial]
                return result
        raise DatabaseError("cannot retrieve data from closed database")

    def _simplify(self, object, result):
        if len(object.items) != 1:
            return False
        if object.items[0] == "*":
            return False
        if result is None:
            return False
        return True

    def _execute(self, object):
        if not isinstance(object, QueryObjects):
            raise InstanceError("instance is not a valid QueryObject")

        failed = True
        cursor = self.connection.cursor()
        try:
            cursor.execute(object.query, tuple(object.inputs))
            failed = False
        except sqlite3.OperationalError as exception:
            error_message = exception.args[0]
        except sqlite3.IntegrityError as exception:
            error_message = exception.args[0]

        if failed:
            inputs = list()
            for input in object.inputs:
                if not isinstance(input, (str, blob)):
                    inputs.append(str(input))
                    continue
                inputs.append("'" + str(input) + "'")
            raise QueryError(error_message, object.query, inputs)

        if isinstance(object, RawReadObject):
            result = cursor.fetchall()
            self._results[object.serial] = result

        if isinstance(object, GetObject):
            if object.get_type == "first":
                result = cursor.fetchone()
                if self._simplify(object, result):
                    result = result[0]
            elif object.get_type == "all":
                result = cursor.fetchall()
                # TODO: simplify single row lists
                if self._simplify(object, result):
                    pass
            self._results[object.serial] = result

        cursor.close()

        if isinstance(object, WriteObjects):
            self.connection.commit()

        if object.await_completion or (object.await_completion is None and self.await_completion):
            self._awaited.append(object.serial)

    def _executions(self):
        while self.alive:
            if self._toRead:
                object = self._toRead[0]
                self._execute(object)
                index = self._toRead.index(object)
                self._toRead.pop(index)
            elif self._toWrite:
                object = self._toWrite[0]
                self._execute(object)
                index = self._toWrite.index(object)
                self._toWrite.pop(index)

# TODO: pragma values
class DatabaseObject(ExecuctionObject):
    """ A database object which allows interaction with an SQLite database.

        Parameters
         - path: The path to the database file.
         - daemon: Whether the database should run in a separate thread.
         - await_completion - Whether the database should wait for queries to complete before returning.
    """

    alive = False

    def __init__(self, path, daemon=True, await_completion=True):
        super(DatabaseObject, self).__init__()
        if not path.endswith(".db"):
            path += ".db"
        self.path = path
        self.name = path.replace("\\", "/").split("/")[-1][:-5]
        self.daemon = daemon
        self.await_completion = await_completion
        self.start()

    def table(self, name):
        """ Returns a table object.
                Parameters
                - name: The name of the table.
            """
        table = TableObject(self, name)
        if not table.exists:
            raise TableError("table does not exist")
        return table

    def create(self, name, columns=None, await_completion=True, must_not_exist=False, **kwargs):
        """ Creates a table within the database.

            Parameters
             - name: The name of the table.
             - columns: A dictionary containting the columns and their types.
             - await_completion: Whether to wait for the table to be created before returning.
             - must_not_exist: Whether to raise an error if the table already exists.
             - kwargs: Alternatively, the columns and their types can be passed as keyword arguments.

             NOTE: If both columns and kwargs are passed, kwargs will be used.
             NOTE: kwargs cannot share the same name with a named arg or kwarg.
        """

        if kwargs:
            columns = kwargs

        if must_not_exist:
            if TableObject(self, name).exists:
                raise TableError("table already exists")
        return CreateTableObject(self, name, columns).run(await_completion)

    # TODO: optimse database
    def optimise(self):
        raise NotImplemented("database optimising has not yet been implemented")

    @property
    def tables(self):
        """ Returns a list of all tables in the database. """
        self.waitForQueue()
        query = f"SELECT name FROM sqlite_master WHERE type='table'"
        tables = list()
        for item in RawReadObject(query, database=self).run():
            tables.append(item[0])
        return tables

    @property
    def queue(self):
        """ Returns the number of queries in the queue. """
        return len(self._toRead + self._toWrite)

    def start(self):
        """ Initiates a connection to the database. """
        if self.alive:
            raise DataError("DatabaseObject has already been started")
        self.alive = True
        threading.Thread(target=self._executions, daemon=self.daemon).start()
        self.connection = sqlite3.connect(self.path, check_same_thread=False)

    def close(self, ignore_queue=False):
        """ Closes the connection to the database. """
        if not self.alive:
            raise DataError("DatabaseObject is already closed")
        if not ignore_queue:
            self.waitForQueue()
        self.alive = False
        self._toRead = list()
        self._toWrite = list()
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        tables = len(self.tables())
        return f"<{self.name} database object: {tables} tables>"

class TableObject:
    """ A table object is used to interact with a table within a database.

        Parameters
         - database: The database object to use.
         - table: The name of the table.
    """

    def __init__(self, database, table):
        super(TableObject, self).__init__()
        if not isinstance(database, DatabaseObject):
            raise InstanceError("instance is not a DatabaseObject")

        self.database = database
        self.name = table

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.database.close()

    def rename(self, name):
        """ Renames the table.

            Parameters
             - name: The new name of the table.
        """
        if name in self.database.tables:
            raise TableError("table already exists")
        query = f"ALTER TABLE {self.name} RENAME TO '{name}'"
        RawWriteObject(query, table=self).run()
        self.name = name

    def delete(self):
        """ Deletes the table. """
        query = f"DROP TABLE {self.name}"
        RawWriteObject(query, table=self).run()
        del self

    def addColumn(self, *values, **kwargs):
        """ Adds a column to the table.

            Parameters
             - values: A dictionary containg the column name and its types.
             - kwargs: Alternatively, the column and its type can be passed as a keyword argument.
        """
        # TODO: refitting table
        AddColumnObject(self, values, refit=False, **kwargs).run()

    # TODO: remove column
    def removeColumn(self):
        """ Removes a column from the table.

            Note: This method is not yet implemented.
        """
        raise NotImplemented("removing columns has not yet been implemented")

    def add(self, values=None, **kwargs):
        """ Adds a row to the table.

            Parameters
                - values: A dictionary of columns and the values to add.
        """
        AddRowObject(self, values, **kwargs).run()

    def remove(self):
        """ Starts a query to remove a row from the table. """
        return RemoveRowObject(self)

    def get(self, *items):
        """ Starts a query to get the first result from the table.

            Parameters
             - items: The columns to get. If none are specified, all columns will be returned.
        """
        return GetObject(self, "first", *items)
    getFirst = get

    def getAll(self, *items):
        """ Starts a query to get all results from the table.

            Parameters
             - items: The columns to get. If none are specified, all columns will be returned.
        """
        return GetObject(self, "all", *items)

    def set(self, *values, **kwargs):
        """ Starts a query to set a value in the table.

            Parameters
             - values: A dictionary of columns and the values to set.
        """
        return SetObject(self, *values, **kwargs)

    @property
    def exists(self):
        """ Returns whether the table exists. """
        self.database.waitForQueue()
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.name}'"
        return bool(RawReadObject(query, table=self).run())

    @property
    def columns(self):
        """ Returns a list of all columns in the table. """
        self.database.waitForQueue()
        query = f"SELECT name FROM PRAGMA_TABLE_INFO('{self.name}')"
        columns = list()
        for item in RawReadObject(query, table=self).run():
            columns.append(item[0])
        return columns

    @property
    def columns_types(self):
        """ Returns a list of all columns and their types in the table. """
        self.database.waitForQueue()
        query = f"SELECT name, type FROM PRAGMA_TABLE_INFO('{self.name}')"
        columns = dict()
        for item in RawReadObject(query, table=self).run():
            columns[item[0]] = item[1]
        return columns

    @property
    def rows(self):
        """ Returns the number of rows in the table. """
        self.database.waitForQueue()
        query = f"SELECT COUNT(*) AS count FROM {self.name}"
        return RawReadObject(query, table=self).run()[0][0]

    def __repr__(self):
        columns = len(self.columns)
        return f"<{self.name} table object: {columns} columns, {self.rows} rows>"

class QueryObject:

    def __init__(self):
        super(QueryObject, self).__init__()
        if not isinstance(self, QueryObjects):
            raise InstanceError("instance is not a valid QueryObject")

    def run(self, await_completion=None):
        if not self.database.alive:
            raise DatabaseError("cannot run query in a closed database")
        self.await_completion = await_completion
        return self._run()

    async def asyncRun(self):
        return self.run(await_completion=True)

    @property
    def query(self):
        if not isinstance(self, QueryObjects):
            raise InstanceError("instance is not a valid QueryObject")
        self.inputs = list()
        if hasattr(self, "serial"):
            return self._query
        query = self._query
        for input in self.inputs:
            query = query.replace("?", input, 1)
        return query

    def __repr__(self):
        return f"<{self.type} query object>"

class LogicObject:

    def __init__(self, object, item, conjunctive=None):
        super(LogicObject, self).__init__()
        if not isinstance(object, LogicObjects):
            raise InstanceError("instance is not a valid LogicObject")

        self.object = object
        self.item = item
        self.conjunctive = conjunctive

    def eq(self, value):
        """ Checks if the value is equal to the specified value. """
        self.operation = "="
        self.value = value
        return self._filter
    equalto = equal = eq

    def neq(self, value):
        """ Checks if the value is not equal to the specified value. """
        self.operation = "!="
        self.value = value
        return self._filter
    notequalto = notequal = eq

    def gt(self, value):
        """ Checks if the value is greater than the specified value. """
        self._isNumber(value)
        self.operation = ">"
        self.value = value
        return self._filter
    greaterthan = gt

    def lt(self, value):
        """ Checks if the value is less than the specified value. """
        self._isNumber(value)
        self.operation = "<"
        self.value = value
        return self._filter
    lessthan = lt

    def gteq(self, value):
        """ Checks if the value is greater than or equal to the specified value. """
        self._isNumber(value)
        self.operation = ">="
        self.value = value
        return self._filter
    greaterthanorequalto = gteq

    def lteq(self, value):
        """ Checks if the value is less than or equal to the specified value. """
        self._isNumber(value)
        self.operation = "<="
        self.value = value
        return self._filter
    lessthanorequalto = lteq

    def like(self, value):
        """ Checks if the value is similar to the specified value. """
        self._isString(value)
        self.operation = "LIKE"
        self.value = value
        return self._filter

    def nlike(self, value):
        """ Checks if the value is not similar to the specified value. """
        self._isString(value)
        self.operation = "NOT LIKE"
        self.value = value
        return self._filter
    notlike = nlike

    def contains(self, value):
        """ Checks if the value contains the specified value. """
        self._isString(value)
        self.operation = "LIKE"
        self.value =  "%" + value + "%"
        return self._filter

    def ncontains(self, value):
        """ Checks if the value does not contain the specified value. """
        self._isString(value)
        self.operation = "NOT LIKE"
        self.value =  "%" + value + "%"
        return self._filter
    notcontains = ncontains

    def startswith(self, value):
        """ Checks if the value starts with the specified value. """
        self._isString(value)
        self.operation = "LIKE"
        self.value = value + "%"
        return self._filter

    def nstartswith(self, value):
        """ Checks if the value does not start with the specified value. """
        self._isString(value)
        self.operation = "NOT LIKE"
        self.value = value + "%"
        return self._filter
    notstartswith = nstartswith

    def endswith(self, value):
        """ Checks if the value ends with the specified value. """
        self._isString(value)
        self.operation = "LIKE"
        self.value = "%" + value
        return self._filter

    def nendswith(self, value):
        """ Checks if the value does not end with the specified value. """
        self._isString(value)
        self.operation = "NOT LIKE"
        self.value = "%" + value
        return self._filter
    notendswith = nendswith

    def IN(self, *values):
        """ Checks if the value is in the specified values. """
        if isinstance(values[0], (list, tuple, set)):
            values = values[0]
        self.operation = "IN"
        self.value = values
        return self._filter

    def NOTIN(self, *values):
        """ Checks if the value is not in the specified values. """
        if isinstance(values[0], (list, tuple, set)):
            values = values[0]
        self.operation = "NOT IN"
        self.value = values
        return self._filter

    def _isNumber(self, value):
        if not isinstance(value, (int, float)):
            raise TypeError("comparison number must be an integer or float")

    def _isString(self, value):
        if not isinstance(value, (str, blob)):
            raise TypeError("comparison item must be a string")

    @property
    def _filter(self):
        self.object.filtered.append(self)
        return self.object

    @property
    def _logic(self):
        logic = ""
        if self.conjunctive:
            logic += " " + self.conjunctive
        logic += f" {self.item} " + self.operation
        if self.operation in ["IN", "NOTIN"]:
            for value in self.value:
                self.object.inputs.append(value)
            logic += "(" + ", ".join("?" * len(self.value)) + ")"
        else:
            logic += " ?"
            self.object.inputs.append(self.value)
        return logic

    def __repr__(self):
        return f"<{self.object.type} logic object>"

class FilterObject:

    def __init__(self):
        super(FilterObject, self).__init__()
        if not isinstance(self, QueryObject):
            raise InstanceError("instance is not a QueryObject")

        self.filtered = list()

    def where(self, item):
        """ Filters the query based on the specified item. """
        if not self.filtered:
            return LogicObject(self, item)
        raise LogicError("already performing logic")

    def eq(self, value):
        """ Checks if the value is equal to the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).eq(value)

    def neq(self, value):
        """ Checks if the value is not equal to the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).neq(value)

    def gt(self, value):
        """ Checks if the value is greater than the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).gt(value)
    greaterthan = gt

    def lt(self, value):
        """ Checks if the value is less than the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).lt(value)
    lessthan = lt

    def gteq(self, value):
        """ Checks if the value is greater than or equal to the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).gteq(value)
    greaterthanorequalto = gteq

    def lteq(self, value):
        """ Checks if the value is less than or equal to the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).lteq(value)
    lessthanorequalto = lteq

    def like(self, value):
        """ Checks if the value is similar to the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).like(value)

    def nlike(self, value):
        """ Checks if the value is not similar to the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).nlike(value)

    def contains(self, value):
        """ Checks if the value contains the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).contains(value)

    def ncontains(self, value):
        """ Checks if the value does not contain the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).ncontains(value)
    notcontains = ncontains

    def startswith(self, value):
        """ Checks if the value starts with the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).startswith(value)

    def nstartswith(self, value):
        """ Checks if the value does not start with the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).nstartswith(value)
    notstartswith = nstartswith

    def endswith(self, value):
        """ Checks if the value ends with the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).endswith(value)

    def nendswith(self, value):
        """ Checks if the value does not end with the specified value. """
        assert self._isItem
        return LogicObject(self, self._getItem).nendswith(value)
    notendswith = nendswith

    def IN(self, *values):
        """ Checks if the value is in the specified values. """
        assert self._isItem
        return LogicObject(self, self._getItem).IN(*values)

    def NOTIN(self, *values):
        """ Checks if the value is not in the specified values. """
        assert self._isItem
        return LogicObject(self, self._getItem).NOTIN(*values)

    def AND(self, item):
        """ Allows for multiple logic operations to be performed where all operations must be true. """
        if self.filtered:
            return LogicObject(self, item, conjunctive="AND")
        raise LogicError("no item to perform logic on")

    def OR(self, item):
        """ Allows for multiple logic operations to be performed where only one of the operations must be true. """
        if self.filtered:
            return LogicObject(self, item, conjunctive="OR")
        raise LogicError("no item to perform logic on")

    @property
    def _isItem(self):
        if not self.filtered:
            if len(self.items) == 1 and "*" not in self.items:
                return True
            raise LogicError("cannot perform logic on multiple items")
        raise LogicError("no item to perform logic on")

    @property
    def _getItem(self):
        if isinstance(self, GetObject):
            return self.items[0]
        elif isinstance(self, SetObject):
            return list(self.items.keys())[0]
        raise LogicError("instance cannot be used as a LogicObject")

    @property
    def _filter(self):
        if not self.filtered:
            return ""
        query = " WHERE"
        for filter in self.filtered:
            query += filter._logic
        return query

class SortObject:

    def __init__(self):
        super(SortObject, self).__init__()
        if not isinstance(self, QueryObject):
            raise InstanceError("instance is not a valid QueryObject")

        self.order = "DESC"
        self.sorted = list()
        self.sortlimit = None

    def sort(self, *items):
        """ Sorts the items by the specified items.

            Parameters:
             - items: The items to sort by.
        """
        if isinstance(items[0], (list, tuple, set)):
            items = items[0]
        self.sorted = items
        return self

    def asc(self):
        """ Sorts the items in ascending order. """
        if not self.sorted:
            raise SortError("items must be provided to sort by before using asc")
        self.order = "ASC"
        return self

    def desc(self):
        """ Sorts the items in descending order. """
        if not self.sorted:
            raise SortError("items must be provided to sort by before using desc")
        self.order = "DESC"
        return self

    def limit(self, limit):
        """ Limits the number of items returned.

            Parameters:
             - limit: The maximum number of items to return.
        """
        if not self.sorted:
            raise SortError("items must be provided to sort before providing a limit")
        self.sortlimit = limit
        return self

    def _sort(self):
        if not self.sorted:
            return ""
        statement = " ORDER BY "
        statement += ", ".join("?" * len(self.sorted))
        self.inputs += self.sorted
        statement += " " + self.order
        if self.sortlimit is not None:
            statement += " LIMIT " + str(self.sortlimit)
        return statement

class RawReadObject(QueryObject):
    """ Allows for raw read queries to be performed.

        Parameters:
         - rawquery: The raw query to perform.
         - table: The table of the database to perform the query on.
         - database: The database to perform the query on.
    """

    type = "raw read"

    def __init__(self, rawquery, table=None, database=None):
        super(RawReadObject, self).__init__()
        if table is not None:
            if not isinstance(table, TableObject):
                raise InstanceError("instance is not a TableObject")
            database = table.database
        elif database is not None:
            if not isinstance(database, DatabaseObject):
                raise InstanceError("instance is not a DatabaseObject")

        self.database = database
        self.rawquery = rawquery

    def _run(self):
        return self.database._read(self)

    @property
    def _query(self):
        return self.rawquery

class RawWriteObject(QueryObject):
    """ Allows for raw write queries to be performed.

        Parameters:
         - rawquery: The raw query to perform.
         - table: The table of the database to perform the query on.
         - database: The database to perform the query on.
    """

    type = "raw write"

    def __init__(self, rawquery, table=None, database=None):
        super(RawWriteObject, self).__init__()
        if table is not None:
            if not isinstance(table, TableObject):
                raise InstanceError("instance is not a TableObject")
            database = table.database
        elif database is not None:
            if not isinstance(database, DatabaseObject):
                raise InstanceError("instance is not a DatabaseObject")

        self.database = database
        self.rawquery = rawquery

    def _run(self):
        return self.database._write(self)

    @property
    def _query(self):
        return self.rawquery

class CreateTableObject(QueryObject):

    type = "create table"

    def __init__(self, database, table, columns, **kwargs):
        super(CreateTableObject, self).__init__()
        if not isinstance(database, DatabaseObject):
            raise InstanceError("instance is not a DatabaseObject")

        if not (columns or kwargs):
            raise InputError("you must provide columns for the table")

        if kwargs:
            columns = kwargs

        items = dict()
        for item in columns:
            value = columns[item]
            if isinstance(value, primary):
                if value.autoincrement and value.type is not int:
                    raise TypeError("primary keys with autoincrementation must be integers")
                elif value.type not in [str, blob, int, float]:
                    raise TypeError(f"'{value.type}' is an invalid data type")
                items[item] = value
            elif isinstance(value, foreign):
                if type(value.table) is str:
                    if value.table not in database.tables:
                        raise InstanceError(f"table '{value.table}' does not exist")
                    value.table = database.table(value.table)
                elif not isinstance(value.table, TableObject):
                    raise InstanceError("table for foreign key is not a table name or a TableObject")
                elif value.type not in [str, blob, int, float]:
                    raise TypeError(f"'{value.type}' is an invalid data type")
                if value.column is None:
                    value.column = item
                elif type(value.column) is not str:
                    raise TypeError("column for foreign key is not a string or None")
                if value.column not in value.table.columns:
                    raise InstanceError(f"column '{value.column}' does not exist")
                value.type = value.table.columns_types[value.column]
                items[item] = value
            elif isinstance(value, (unique, default, null, notnull)):
                if value.type not in [str, blob, int, float]:
                    raise TypeError(f"'{value.type}' is an invalid data type")
                items[item] = value
            elif value is primary:
                items[item] = primary()
            elif value in [str, blob, int, float]:
                items[item] = null(value)
            else:
                raise TypeError(f"'{value}' is an invalid data type")

        self.database = database
        self.name = table
        self.items = items

    def _run(self):
        self.database._write(self)
        return TableObject(self.database, self.name)

    @property
    def _query(self):
        query = f"CREATE TABLE IF NOT EXISTS {self.name} ("
        lines = list()
        autoincrement = False
        primaries = list()
        for item in self.items:
            line = item + " "
            value = self.items[item]
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
                    line += " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"
                else:
                    line += " NOT NULL"
                primaries.append(item)
            elif isinstance(value, foreign):
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

class AddColumnObject(QueryObject):

    type = "add column"

    def __init__(self, table, values, refit=False, **kwargs):
        super(AddColumnObject, self).__init__()
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")

        if not (values or kwargs):
            raise InputError("you must provide a column to be added")

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

        self.database = table.database
        self.table = table
        self.refit = refit
        self.name = name
        self.value = value

    def andAdd(self, **kwargs):
        for item in kwargs:
            self.items[item] = kwargs[item]
        return self

    def _run(self):
        return self.database._write(self)

    @property
    def _query(self):
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

class AddRowObject(QueryObject):

    type = "add row"

    def __init__(self, table, values, **kwargs):
        super(AddRowObject, self).__init__()
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")

        if not (values or kwargs):
            raise InputError("you must provide values to be added")

        if kwargs:
            values = kwargs

        self.database = table.database
        self.table = table
        self.items = values.copy()

    # essentially useless
    def andAdd(self, **kwargs):
        for item in kwargs:
            self.items[item] = kwargs[item]
        return self

    def _run(self):
        return self.database._write(self)

    @property
    def _query(self):
        query = f"INSERT INTO {self.table.name} ({', '.join(self.items)}) "
        query += f"VALUES ({', '.join('?' * len(self.items))})"
        self.inputs = list(self.items.values())
        return query

class RemoveRowObject(QueryObject, FilterObject):

    type = "remove row"

    def __init__(self, table):
        super(RemoveRowObject, self).__init__()
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")

        self.database = table.database
        self.table = table

    def _run(self):
        return self.database._write(self)

    @property
    def _query(self):
        query = f"DELETE FROM {self.table.name}"
        return query + self._filter

class GetObject(QueryObject, FilterObject, SortObject):

    type = "get row"

    def __init__(self, table, get_type, *items):
        super(GetObject, self).__init__()
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")

        if get_type not in ["first", "all"]:
            raise TypeError("invalid get type")

        if not items:
            items = ["*"]
        elif isinstance(items[0], (list, tuple, set)):
            items = items[0]

        self.database = table.database
        self.table = table
        self.get_type = get_type
        self.items = items

    def andGet(self, *items):
        if "*" in self.items:
            return self
        if isinstance(items[0], (list, tuple, set)):
            items = items[0]
        self.items += items
        return self

    def _run(self):
        return self.database._read(self)

    @property
    def _query(self):
        query = f"SELECT {', '.join(self.items)} FROM {self.table.name}"
        return query + self._filter + self._sort()

class SetObject(QueryObject, FilterObject, SortObject):

    type = "set row"

    def __init__(self, table, values=None, **kwargs):
        super(SetObject, self).__init__()
        if not isinstance(table, TableObject):
            raise InstanceError("instance is not a TableObject")

        if not (values or kwargs):
            raise InputError("you must provide values to be set")

        if kwargs:
            values = kwargs

        items = dict()
        for item in values:
            items[item] = values[item]

        self.database = table.database
        self.table = table
        self.items = items

    def andSet(self, **kwargs):
        for item in kwargs:
            self.items[item] = kwargs[item]
        return self

    def _run(self):
        return self.database._write(self)

    @property
    def _query(self):
        query = f"UPDATE {self.table.name} SET "
        items = list()
        for item in self.items:
            value = self.items[item]
            if isinstance(value, (str, blob, int, float)):
                if isinstance(value, blob):
                    value = value.value
                items.append(f"{item}=?")
                self.inputs.append(value)
            elif isinstance(value, increment):
                items.append(f"{item}={item}+?")
                self.inputs.append(value.increment)
            elif isinstance(value, concatenate):
                items.append(f"{item}={item} || ?")
                self.inputs.append(value.concatenate)
            elif value is null or isinstance(value, null):
                items.append(f"{item}=NULL")
            else:
                raise TypeError(f"'{type(value)}' is an invalid data type")
        return query + ", ".join(items) + self._filter + self._sort()

QueryObjects = (RawReadObject, RawWriteObject,
                CreateTableObject, AddColumnObject,
                AddRowObject, RemoveRowObject,
                GetObject, SetObject)
LogicObjects = (RemoveRowObject, GetObject, SetObject)
WriteObjects = (RawWriteObject, CreateTableObject, AddColumnObject,
                AddRowObject, RemoveRowObject, SetObject)