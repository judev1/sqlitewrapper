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
table.addColumn(bio=blob)
```

#### Adding and removing rows
```py
table.add(token="arZrJFbECw", username="user1") # id will be 1, and tag will be 0
table.add(token="xpReueejDK", username="user2") # id will be 2, and tag will be 0
table.add(token="JwVSFbpRnI", username="user3")

# can use logical queries so must be run
table.remove().where("id").eq(2).run()
table.remove().where("id").eq(2).run()
```

#### Getting data from the table
```py
# gets all the values in the first row where the token equals 'arZrJFbECw'
result = table.get().where("token").eq("arZrJFbECw").run()

# gets all the usernames in every row
result = table.getAll("username").run()
```
#### Updating data in the table
```py
# sets desc to null where tokens equals 'arZrJFbECw'
table.set(desc=types.null).where("token").eq("arZrJFbECw").run()

# increase tag by 1000 if tag is equal to 0 or 1
table.set(tag=types.increment(1000)).IN(0, 1).run()
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
            "user_id": types.foreign('users'),
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
result = table.getAll("username").run() # in most cases 'user4' won't show up
```