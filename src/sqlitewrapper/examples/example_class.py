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