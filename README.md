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

columns = {
    "id": types.primary(autoincrement=True),
    "token": types.unique(str),
    "username": types.notnull(str),
    "tag": types.default(0),
    "desc": types.blob, # types.null(types.blob) also works
    #"joined": types.primary # types.primary() and types.primary(int) also work, but you can't have multiple primary keys if one of them is autoincrementing
}

db = sqlitewrapper.database("my_database")
table = db.create("my_table", columns)
```

#### Adding and removing columns
```py
db = sqlitewrapper.database("my_database")
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

#### Don't await completion
This is to allow sqlitewrapper to run write queries without the main program having to wait for it's completion

**NOTE:** The query execution rotates attempting to execute a read query and then a write query, this is so the queue is not stacked up preventing one type of query from being executed

**NOTE:** When doing cleanup you can use `db.waitForQueue()`, which will run until the queue is empty
```py
db = sqlitewrapper.database("my_database", await_completion=True)
table = db.table("my_table")

table.add(token="xrDIltpbBQ", username="user4")
result = table.getAll("username").run() # in most cases 'user4' won't show up
```