from hypothesis.strategies import composite, integers, one_of

from src.interpret import _common


@composite
def u8_integers(draw):
    return draw(integers(0, 2**8))


@composite
def positive_u16_integers(draw):
    return draw(integers(1, 2**16))


@composite
def u16_integers(draw):
    return draw(integers(0, 2**16))


@composite
def dont_care_character(draw):
    return _common.DONT_CARE


@composite
def dont_care_integers(draw):
    return draw(one_of(dont_care_character(), u16_integers()))
