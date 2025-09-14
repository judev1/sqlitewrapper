from typing import Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .databaseobjects import DatabaseObject, TableObject

class blob:
    def __init__(self, value):
        self.value = bytes(value, 'utf-8')
    def __add__(self, other) -> 'blob | str':
        if isinstance(other, blob):
            return blob(self.value + other.value)
        elif isinstance(other, str):
            return str(self.value, 'utf-8') + other
        raise TypeError(f"cannot concatenate blob to {type(other).__name__}")
    def __radd__(self, other) -> 'blob | str':
        if isinstance(other, blob):
            return blob(other.value + self.value)
        elif isinstance(other, str):
            return other + str(self.value, 'utf-8')
        raise TypeError(f"cannot concatenate blob to {type(other).__name__}")
    def __repr__(self) -> str:
        return str(self.value, 'utf-8')

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
    type: str
    def __init__(self,
        table: Union['TableObject', str],
        column: Optional[str] = None
    ):
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

def isNumber(value):
    if not isinstance(value, (int, float)):
        raise TypeError("comparison number must be an integer or float")
    return True

def isString(value):
    if not isinstance(value, (str, blob)):
        raise TypeError("comparison item must be a string")
    return True