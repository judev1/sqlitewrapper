from sqlitewrapper import Database, Table, types

db = Database("my_database")

if db.exists("my_table"):
    db.drop("my_table")
if db.exists("items"):
    db.drop("items")

table = db.create(
    "my_table",
    id=types.primary(autoincrement=True),
    token=types.unique(str),
    username=types.notnull(str),
    tag=types.default(0),
    desc=types.blob, # types.null(types.blob) also works
    status=str, # types.null(str) also works
)

table.add(token="arZrJFbECw", username="user1")
table.add(token="xpReueejDK", username="user2")
table.add(token="JwVSFbpRnI", username="user3", tag=2)

# get all values for a single record where the id equals 1
r = table.get().where("id").eq(1).run()
print(r)

# get the username for a single record where the id equals 1
r = table.get("username").where("username").startswith("user").run()
print(r)

# get the usernames for every record
r = table.getAll("username").run()
print(r)

# gets the usernames for every record where the id is less than 3
r = table.getAll().where("id").lt(3).run()
print(r)

# increase tag by 1000 if tag is equal to 0 or 1
table.set(tag=types.increment(2)).where("tag").IN(0, 1).run()

# set status to new if id is greater than or equal to 2
table.set(status="new").where("id").gteq(2).run()

# gets all fields for every record
r = table.getAll().sort('id').desc().run()
print(r)

items = db.create(
    "items",
    id=types.primary(autoincrement=True),
    user_id=types.foreign(table),
    name=types.notnull(str),
)

items.add(user_id=1, name="item1")

r = table.getAll("username").sort("username").run()
print(r)

r = table.getAll("username").sort("username").asc().run()
print(r)

r = table.getAll("username").sort("username").limit(2).run()
print(r)

# specify table names for columns when column name have conflicts
r = items.get('my_table.id', 'items.id', 'name', 'status').join().run()
print(r)

# if there are multiple foreign keys, specify which one
r = items.get().join('user_id').run()
print(r)

# if the key isn't a foreign key, specify both keys
r = items.get().join('user_id', 'my_table.id').run()
print(r)

# using conditions after joining a table
r = items.get().join().where('items.id').eq(1).run()
print(r)

items.add(user_id=1, name="item2")
items.add(user_id=1, name="item3")
items.add(user_id=2, name="item4")
items.add(user_id=3, name="item5")
items.add(user_id=3, name="item6")

a = items.getAll("my_table.id", "items.id", "name").join()
print(a.run())

b = a.where("items.id").lt(5)
c = b.AND("my_table.id").gteq(2)
print(b.run())
print(b.run())
print(c.run())

c = a.where("my_table.id").gt(2)
print(c.run())