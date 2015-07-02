# -*- coding: utf-8 -*-

__all__ = ['KiwiError',
           'ArgumentError',
           'InvalidRequestError',
           'NoPrimaryKeyError', ]


class KiwiError(Exception):
    """ Generic error class."""


class ArgumentError(KiwiError):
    pass


class InvalidRequestError(KiwiError):
    pass


class NoPrimaryKeyError(InvalidRequestError):
    pass
