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

class NotImplemented(DatabaseError):
    pass

class QueryError(DatabaseError):

    def __init__(self, error_message, query, inputs):
        self.error_message = error_message
        self.query = query
        self.inputs = inputs

    def __str__(self):
        message = self.error_message
        message += "\nOn query:\t" + self.query()
        if self.inputs:
            message += "\nWith values:\t" + ", ".join(self.inputs)
        return message