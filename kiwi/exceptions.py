# -*- coding: utf-8 -*-

__all__ = ['KiwiError', 'ArgumentError', 'InvalidRequestError',
        'NoPrimaryKeyError',
        ]

class KiwiError(Exception):
    """ Generic error class."""

class ArgumentError(KiwiError):
    """ Raised when an invalid or conflicting function argument is supplied. """

class InvalidRequestError(KiwiError):
    """ Raised when an error occurs during query parsing """

class NoPrimaryKeyError(InvalidRequestError):
    pass

