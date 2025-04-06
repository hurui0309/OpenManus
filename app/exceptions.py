"""Custom exceptions for the application."""

class DatabaseError(Exception):
    """Raised when a database operation fails."""
    pass

class ReviewError(Exception):
    """Raised when SQL review operation fails."""
    pass

class ToolError(Exception):
    """Raised when a tool encounters an error."""

    def __init__(self, message):
        self.message = message


class OpenManusError(Exception):
    """Base exception for all OpenManus errors"""


class TokenLimitExceeded(OpenManusError):
    """Exception raised when the token limit is exceeded"""
