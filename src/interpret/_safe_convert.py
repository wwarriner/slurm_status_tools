from typing import Optional, Type, TypeVar, Union

T = TypeVar("T")
Numeric = Union[int, float]
NumericT = TypeVar("NumericT", bound=Numeric)


def convert_value_to_bool_safe(_true_v: str, _false_v: str, _v: str) -> Optional[bool]:
    if _v == _true_v:
        out = True
    elif _v == _false_v:
        out = False
    else:
        out = None
    return out


def convert_value_unsafe_to_none(_unsafe_v: T, _v: T) -> Optional[T]:
    return convert_value_unsafe_to_safe(_unsafe_v, None, _v)


def convert_value_unsafe_to_safe(
    _unsafe_v: T, _safe_v: Union[T, None], _v: Optional[T]
) -> Optional[T]:
    if _v == _unsafe_v or _v is None:
        out = _safe_v
    else:
        out = _v
    return out


def type_cast_float_unsafe_to_none(_v: Union[str, None]) -> Optional[float]:
    return type_cast_value_unsafe_to_safe(float, None, _v)


def type_cast_int_unsafe_to_none(_v: Union[str, None]) -> Optional[int]:
    return type_cast_value_unsafe_to_safe(int, None, _v)


def type_cast_value_unsafe_to_none(_type: Type[T], _v: Union[str, None]) -> Optional[T]:
    return type_cast_value_unsafe_to_safe(_type, None, _v)


def type_cast_value_unsafe_to_safe(
    _type: Type[T], _safe_v: Union[T, None], _v: Union[str, None]
) -> Optional[T]:
    try:
        out = _type(_v)
    except:
        out = _safe_v
    return out


def clip_negative(_v: Optional[NumericT]) -> Optional[NumericT]:
    return restrict_negative_value_to_safe(0.0, _v)  # type: ignore


def restrict_negative_value_to_none(_v: Optional[NumericT]) -> Optional[NumericT]:
    return restrict_negative_value_to_safe(None, _v)


def restrict_negative_value_to_safe(
    _safe_v: Optional[NumericT], _v: Optional[NumericT]
) -> Optional[NumericT]:
    if _v is None or _v < 0.0:
        out = _safe_v
    else:
        out = _v
    return out
