class blob:
    def __init__(self, value):
        self.value = str(value)
    def __repr__(self):
        return self.value

class increment:
    def __init__(self, increment=1):
        self.increment = increment

class concatenate:
    def __init__(self, concatenate=""):
        self.concatenate = concatenate
concat = concatenate

class primary:
    def __init__(self, type=int, autoincrement=False):
        if autoincrement and type is not int:
            autoincrement = False
        self.type = type
        self.autoincrement = autoincrement
primary_key = primary

class foreign:
    def __init__(self, table, column=None):
        self.table = table
        self.column = column
foreign_key = foreign

class unique:
    def __init__(self, type):
        self.type = type

class default:
    def __init__(self, value):
        self.type = type(value)
        self.value = value

class null:
    def __init__(self, type):
        self.type = type

class notnull:
    def __init__(self, type):
        self.type = type