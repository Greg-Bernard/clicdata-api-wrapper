class ObjectNotFound(Exception):
    """
    An exception risen when an expected object is not found
    """

class InvalidRecId(Exception):
    """
    An exception risen when a provided RecId does not exist
    """


class InvalidDataType(Exception):
    """
    An exception risen when data type provided is not supported
    """


class ConnectionError(Exception):
    """
    An exception risen when there is an issue with the connection
    """