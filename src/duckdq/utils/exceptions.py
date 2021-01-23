#TODO: clean up unused exceptions, merge similar ones and structure exceptions in categories (subclasses)
class DuckDQException(Exception):
    pass

class UnknownStateTypeException(DuckDQException):
    pass

class UnknownOperatorTypeException(DuckDQException):
    pass

class UnsupportedConnectionObjectException(DuckDQException):
    pass

class UnsupportedPropertyException(DuckDQException):
    pass

class StateTypeMismatchException(DuckDQException):
    pass

class StateHandlerUnsupportedStateException(DuckDQException):
    pass

class StateHandlerNotSupportedByEngine(DuckDQException):
    pass

class UnsupportedResultTypeException(DuckDQException):
    pass

class UnsupportedStateTypeException(DuckDQException):
    pass

class PreconditionNotMetException(DuckDQException):
    pass

class EmptyStateException(DuckDQException):
    pass

class NoMetricForValueException(DuckDQException):
    pass

class MetricTypeNotSupportedException(DuckDQException):
    pass

class DataQualityException(DuckDQException):
    pass

class StateMergingException(DuckDQException):
    pass
