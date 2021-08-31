class DatabaseError(Exception):
    pass

class TableError(DatabaseError):
    pass

class InstanceError(DatabaseError):
    pass

class SortError(DatabaseError):
    pass

class LogicError(DatabaseError):
    pass

class InputError(DatabaseError):
    pass

class TypeError(InputError):
    pass

class QueryError(DatabaseError):

    def __init__(self, error_message, query, inputs):

        self.error_message = error_message
        self.query = query
        self.inputs = inputs

    def __str__(self):
        message = self.error_message
        message += "\nON QUERY: " + self.query
        if self.inputs:
            message += "\nWITH VALUES: " + ", ".join(self.inputs)
        return message