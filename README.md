# sqlitewrapper
A python object-oriented wrapper for sqlite, based on sqlite3

## Key features
sqlitewrapper covers a wide range of queries, with a few additional features
 - Easily create and run queries
 - Easily intergrate queries into programs
 - Database and table methods
 - Thread safe

## Installation
```
pip install sqlitewrapper
```

## Examples

#### Creating a table
```py
import sqlitewrapper
from sqlitewrapper import types

db = sqlitewrapper.open("my_database")
table = db.create("my_table", {
    "id": types.primary(autoincrement=True),
    "token": types.unique(str),
    "username": types.notnull(str),
    "tag": types.default(0),
    "desc": types.blob, # types.null(types.blob) also works
    "status": str, # types.null(str) also works
    #"joined": types.primary # types.primary() and types.primary(int) also work, but you can't have multiple primary keys if one of them is autoincrementing
})
```

#### Adding and removing columns
```py
db = sqlitewrapper.open("my_database")
table = db.table("my_table")

table.removeColumn("desc")
table.addColumn(bio=types.blob)
```

#### Adding and removing rows
```py
table.add(token="arZrJFbECw", username="user1") # id will be 1, and tag will be 0
table.add(token="xpReueejDK", username="user2") # id will be 2, and tag will be 0
table.add(token="JwVSFbpRnI", username="user3") # and so on...

# statements which can use logical queries must be run
table.remove().where("id").eq(2).run()
```

#### Getting data from the table
```py
# gets all the values of the first match where the token equals 'arZrJFbECw'
result = table.get().where("token").eq("arZrJFbECw").run()

# gets the username of the first match where the token equals 'arZrJFbECw'
result = table.get("username").where("token").eq("arZrJFbECw").run()

# gets all the usernames where the token equals 'arZrJFbECw'
result = table.getAll("username").where("token").eq("arZrJFbECw").run()
```

#### Sorting data
```py
# Gets all the usernames in descending order
result = table.getALL("username").sort("username").desc().run()

# Gets the first three usernames by username
result = table.getALL("username").sort("username").limit(3).run()
```

#### Updating data in the table
```py
# sets desc to null where token equals 'arZrJFbECw'
table.set(desc=types.null).where("token").eq("arZrJFbECw").run()

# increases tag by 1000 if tag is equal to 0 or 1
table.set(tag=types.increment(1000)).IN(0, 1).run()

# increases tag by 500 if tag is less than 500
table.set(tag=types.increment(500)).lt(500).run()
```

#### Within a class
```py
from sqlitewrapper import DatabaseObject, types

class MyDB(DatabaseObject):

    def __init__(self):
        super().__init__("my_database")

        self.create("users", {
            "user_id": types.primary(autoincrement=True),
            "username": types.notnull(str)
        })

        self.create("user_items", {
            "item_id": types.primary(autoincrement=True),
            "user_id": types.foreign('users'), # if column name is not shared than use the column parameter
            "username": types.notnull(str)
        })
```

#### Don't await completion
This is to allow sqlitewrapper to run write queries without the main program having to wait for it's completion

**NOTE:** The query execution rotates attempting to execute a read query and then a write query, this is so the queue is not stacked up preventing one type of query from being executed

**NOTE:** When doing cleanup you can use `db.waitForQueue()`, which will run until the queue is empty
```py
db = sqlitewrapper.open("my_database", await_completion=True)
table = db.table("my_table")

table.add(token="xrDIltpbBQ", username="user4")
result = table.getAll("username").run() # returns None
```

## Other features
Features which make the wrapper more accessible

#### Getting tables
```py
tables = database.tables() # returns a list of the tables in the database
```

#### Table management
```py
table.rename("table 2.0")
table.delete() # may still be less fun than your 'DROP TABLE' statement
```

#### Table meta data
```py
columns = table.columns() # returns a list of all of the columns
columns = table.columntypes() # returns a dict of all of the columns and their types
rows = table.rows() # returns the number of records in the table
```
