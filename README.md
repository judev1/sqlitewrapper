# sqlitewrapper

A python object-oriented wrapper for sqlite, based on sqlite3

## Key features

sqlitewrapper covers a wide range of queries, with a few additional features

- Database, table, and query objects
- Easily create and run queries
- Easily integrate queries into programs
- Perform get, set, and remove queries
- Sort, filter, and join queries
- Manage and interact with databases more easily

## Installation

```
pip install sqlitewrapper
```

## Reference

### Initialising a database

import `sqlitewrapper` and specify the file path. A `.db` extension will be added if not specified. The database will be created if it does not exist.

```py
from sqlitewrapper import Database, Table, types

db = Database("my_database")
```

### Creating a table

A table can be created in a database using the create method and specifying the columns as keyword arguments with the values as the column types. An additional parameter, `must_not_exist`, can be provided to raise an exception if a table already exists, by default `must_not_exist = False`.

```py
table = db.create(
    "my_table",
    id=types.primary(autoincrement=True),
    token=types.unique(str),
    username=types.notnull(str),
    tag=types.default(0),
    desc=types.blob, # types.null(types.blob) also works
    status=str, # types.null(str) also works
)
```

### Opening a table

Tables can be initialised in two ways, if a database is already initialised or if multiple tables are being accessed, the database has a `.table` method to return the table. Otherwise tables can be accessed using the `Table` function.

```py
table = Table("my_database", "my_table")
table = db.table("my_table")
```

### Adding records

`id` has been defined as autoincrementing and does not need to be specified, tag has a default value of 0 but can be overridden, and `profile_pic` and `status are NULL types and can be left blank.

```py
table.add(token="arZrJFbECw", username="user1")
table.add(token="xpReueejDK", username="user2")
table.add(token="JwVSFbpRnI", username="user3", tag=2)
```

### Getting records

Get queries must be `.run` since they can be filtered and sorted using `.where`. When getting a single column from a table, the result will be returned by itself.

```py
# get all values for a single record where the id equals 1
table.get().where("id").eq(1).run()
>>> (1, 'arZrJFbECw', 'user1', 0, None, None)

# get the username for a single record where the id equals 1
table.get("username").where("username").startswith("user").run()
>>> 'user1'

# get the usernames for every record
table.getAll("username").run()
>>> ['user1', 'user2', 'user3']

# gets the usernames for every record where the id is less than 3
table.getAll().where("id").lt(3).run()
>>> [(1, 'arZrJFbECw', 'user1', 0, None, None), (2, 'xpReueejDK', 'user2', 0, None, 'new')]
```

### Updating records

Like get queries, set queries must be run since they can be filtered to include only certain records.

```py
# increase tag by 1000 if tag is equal to 0 or 1
table.set(tag=types.increment(2)).where("tag").IN(0, 1).run()

# set status to new if id is greater than or equal to 2
table.set(status="new").where("id").gteq(2).run()

# gets all fields for every record
table.getAll().sort("id").desc().run()
>>> [(3, 'JwVSFbpRnI', 'user3', 2, None, 'new'), (2, 'xpReueejDK', 'user2', 2, None, 'new'), (1, 'arZrJFbECw', 'user1', 2, None, None)]
```

### Filtering records

Records can be filtered by a number of different methods and used with conjunctives to add more filters. Here are some examples of common filters:

```py
table.get("token").where("user_id").eq(2).run()
table.get("token").where("user_id").neq(2).run()
table.get("token").where("user_id").lt(2).run()
table.get("token").where("user_id").gt(2).run()
table.get("token").where("user_id").lteq(2).run()
table.get("token").where("user_id").gteq(2).run()

table.get("token").where("user_id").IN(1, 2).run()

table.get("token").where("username").eq("user1").run()
table.get("token").where("username").like("user").run()
table.get("token").where("username").contains("user").run()
table.get("token").where("username").startswith("user").run()
table.get("token").where("username").endswith("1").run()
table.get("token").where("username").nendswith("1").run()

table.get("token").where("user_id").neq(2).AND("username").contains("user").run()
table.get("token").where("user_id").eq(2).OR("username").endswith("1").run()
```

### Sorting records

Records can be sorted in descending or ascending order, and can be given a limit to how many records to return.

```py
table.getAll("username").sort("username").run()
>>> ['user3', 'user2', 'user1']

table.getAll("username").sort("username").asc().run()
>>> ['user1', 'user2', 'user3']

table.getAll("username").sort("username").limit(2).run()
>>> ['user3', 'user2']
```

### Removing records

```py
table.remove().where("id").eq(2).run()
table.remove().where("id").eq(2).OR("id").eq(3).run()
```

### Joining tables

By default, join tries to find a foreign key from the left table, raising an error if there are more than one, and joining that with the reference key. A different column can be specified to use if there are multiple foreign keys, and potentially two columns could be specified if the left column is not a foreign key.

If tables share column names, the table name must be provided to specify which table's column to get, separated by a dot like so: `table.column`.

```py
items = db.create(
    "items",
    id=types.primary(autoincrement=True),
    user_id=types.foreign(table),
    name=types.notnull(str),
)

items.add(user_id=1, name="item1")

# specify table names for columns when column name have conflicts
items.get("my_table.id", "items.id", "name", "status").join().run()
>>> (1, 1, 'item1', None)

# if there are multiple foreign keys, specify which one
items.get().join("user_id").run()
>>> (1, 1, 'item1', 1, 'arZrJFbECw', 'user1', 2, None, None)

# if the key isn't a foreign key, specify both keys
items.get().join("user_id", "my_table.id").run()
>>> (1, 1, 'item1', 1, 'arZrJFbECw', 'user1', 2, None, None)

# using conditions after joining a table
items.get().join().where("items.id").eq(1).run()
>>> (1, 1, 'item1', 1, 'arZrJFbECw', 'user1', 2, None, None)
```

### Reusing queries

Query objects can be stored and reused.

```py
items.add(user_id=1, name="item2")
items.add(user_id=1, name="item3")
items.add(user_id=2, name="item4")
items.add(user_id=3, name="item5")
items.add(user_id=3, name="item6")

a = items.getAll("my_table.id", "items.id", "name").join()
print(a.run())
>>> [(1, 1, 'item1'), (1, 2, 'item2'), (1, 3, 'item3'), (2, 4, 'item4'), (3, 5, 'item5'), (3, 6, 'item6')]

b = a.where("items.id").lt(5)
c = b.AND("my_table.id").gteq(2)
print(b.run())
>>> [(1, 1, 'item1'), (1, 2, 'item2'), (1, 3, 'item3'), (2, 4, 'item4')]
print(b.run())
>>> [(1, 1, 'item1'), (1, 2, 'item2'), (1, 3, 'item3'), (2, 4, 'item4')]
print(c.run())
>>> [(2, 4, 'item4')]

c = a.where("my_table.id").gt(2)
print(c.run())
>>> [(3, 5, 'item5'), (3, 6, 'item6')]
```

### Other features

#### Table data

```py
db.tables
>>> ['my_table', 'sqlite_sequence', 'items']

table.primary_keys
>>> ['id']

items.foreign_keys # (table, from, to)
>>> [('my_table', 'user_id', 'id')]

table.columns
>>> ['id', 'token', 'username', 'tag', 'desc', 'status']

table.column_types
>>> {'id': 'INTEGER', 'token': 'TEXT', 'username': 'TEXT', 'tag': 'INTEGER', 'desc': 'BLOB', 'status': 'TEXT'}

table.rows
>>> 3
```

#### Table management

```py
table.rename("users")
db.exists("users")
>>> True

table.delete() # may still be less fun than your 'DROP TABLE' statement
db.exists("users")
>>> False
```

#### Raw queries
```py
from sqlitewrapper.databaseobjects import RawWriteObject, RawReadObject

query = "CREATE TABLE raw (id int)"
RawWriteObject(query, database=db).run()

query = "SELECT * FROM sqlite_master WHERE type='table' AND name='raw'"
RawReadObject(query, database=db).run()
>>> [('table', 'raw', 'raw', 6, 'CREATE TABLE raw (id int)')]
```

#### Don't await completion

This is to allow sqlitewrapper to run write queries without the main program having to wait for it's completion

**NOTE:** I have made changes to the database classes and haven't properly retested the functionality of threads and awaiting. As such, I would recommend using the default settings. (`separate_thread = False`, `await_completion = True`)

**NOTE:** The query execution rotates attempting to execute a read query and then a write query, this is so the queue is not stacked up preventing one type of query from being executed

**NOTE:** When doing cleanup you can use `db.waitForQueue()`, which will run until the queue is empty

```py
db = Database("my_database", await_completion=True)
table = db.table("my_table")

table.add(token="xrDIltpbBQ", username="user4")
result = table.getAll("username").run() # in most cases 'user4' won't show up
db.waitForQueue() # ensure the queue completes execution before the program finishes
```

## Example class

If you decide to integrate the functionality of the database into your own class, you can use the `DatabaseObject` class to inherit from.

```py
from sqlitewrapper import DatabaseObject, types

class Database(DatabaseObject):

    def __init__(self):
        super().__init__("user_database")

        self.users = self.create("users", {
            "user_id": types.primary(autoincrement=True),
            "username": types.unique(str)
        })

        self.items = self.create("user_items", {
            "item_id": types.primary(autoincrement=True),
            "user_id": types.foreign('users'),
            "item": types.notnull(str),
        })

    def add_user(self, username: str):
        self.users.add(username=username) # will raise an error if the username is not unique

    def add_item(self, username: str, item: str):
        user_id = self.users.get("user_id").where("username").eq(username).run()
        if not user_id:
            raise Exception("No user found with that username")
        self.items.add(user_id=user_id, item=item)

    def get_items(self, usernames: list[str]) -> list[tuple[int, str, str]]:
        items = []
        q = self.items.getAll("username", "item_id", "item").join()
        for username in usernames:
            r = q.where("username").eq(username).run()
            if not r:
                continue
            items.extend(r)
        return items

if __name__ == "__main__":
    db = Database()
    users = ["userABC", "user123", "userDoReMi"]
    items = ["first", "second", "third"]
    for user in users:
        db.add_user(user)
        for item in items:
            db.add_item(user, f"{user}'s {item} item")
    items = db.get_items(["userABC", "userDoReMi"])
    print(items)
```
