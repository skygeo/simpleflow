class MissingResource(Exception):
    pass


class InvalidDataFormat(Exception):
    pass


class MalformedFileNameError(Exception):
    pass


class InvalidUrlException(Exception):
    pass


class ElasticSearchIncompleteIndex(Exception):
    pass


class BotifyQueryException(Exception):
    pass


class ConfigurationError(Exception):
    pass


class ApiError(Exception):
    pass


#raised when the format returned by the API is wrong (or unexpected)
class ApiFormatError(Exception):
    pass


class ErrorRateLimitExceeded(Exception):
    """Raised when error occurs during document pushing
    """
    pass


class HostDoesNotExist(Exception):
    pass


class InvalidCSVQueryException(Exception):
    pass


class InvalidCSVQueryExceptionTooManyFields(InvalidCSVQueryException):
    pass


class InvalidCSVQueryExceptionTooManyMultiples(InvalidCSVQueryException):
    pass
