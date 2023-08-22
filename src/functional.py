from __future__ import annotations

from functools import partial, reduce
from typing import Callable, Generic, Iterable, Type, TypeVar

from typing_extensions import ParamSpec

SM = TypeVar("SM")
TM = TypeVar("TM", covariant=True)

SG = TypeVar("SG")
TG = TypeVar("TG")
UG = TypeVar("UG")

PG = ParamSpec("PG")


class MonadicFunction(Generic[SM, TM]):
    def __init__(self, _fn: Callable[[SM], TM]) -> None:
        self._fn: Callable[[SM], TM] = _fn

    def __call__(self, _s: SM) -> TM:
        return self._fn(_s)

    def lcompose(self, _outer: Callable[[TM], UG]) -> MonadicFunction[SM, UG]:
        return MonadicFunction(self._compose(_outer, self._fn))

    def rcompose(self, _inner: Callable[[UG], SM]) -> MonadicFunction[UG, TM]:
        return MonadicFunction(self._compose(self._fn, _inner))

    @staticmethod
    def _compose(
        _outer: Callable[[TG], UG],
        _inner: Callable[[SG], TG],
    ) -> Callable[[SG], UG]:
        return lambda x: _outer(_inner(x))

    @classmethod
    def identity(cls: Type[MonadicFunction[SG, SG]]) -> MonadicFunction[SG, SG]:
        return cls(cls._identity)

    @classmethod
    def apply(
        cls: Type[MonadicFunction[SG, SG]], _fn: Callable[[SG], SG]
    ) -> "MonadicFunction[SG, SG]":
        return cls(partial(cls._apply, _fn=_fn))

    @classmethod
    def map(
        cls: Type[MonadicFunction[Iterable[SG], Iterable[TG]]],
        _fn: Callable[[SG], TG],
    ) -> MonadicFunction[Iterable[SG], Iterable[TG]]:
        return cls(partial(cls._map, _fn=_fn))

    @classmethod
    def reduce_l(
        cls: Type[MonadicFunction[Iterable[SG], TG]],
        _fn: Callable[[TG, SG], TG],
        _initial: TG,
    ) -> MonadicFunction[Iterable[SG], TG]:
        return cls(partial(cls._reduce, _fn=_fn, _initial=_initial))

    @staticmethod
    def _identity(x: SG) -> SG:
        return x

    @staticmethod
    def _apply(_fn: Callable[[SG], SG], _v: SG) -> SG:
        return _fn(_v)

    @staticmethod
    def _map(_fn: Callable[[SG], TG], _v: Iterable[SG]) -> Iterable[TG]:
        return (_fn(item) for item in _v)

    @staticmethod
    def _reduce(_fn: Callable[[TG, SG], TG], _v: Iterable[SG], _initial: TG) -> TG:
        return reduce(_fn, _v, _initial)


def fold_l(*_fn: MonadicFunction) -> MonadicFunction:
    return reduce(MonadicFunction.lcompose, _fn, MonadicFunction.identity())


def noop(*args, **kwargs) -> None:
    pass
