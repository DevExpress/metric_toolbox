import pytest
from toolbox.utils.decorator_factory import decorator


@decorator
def my_wrapper(func, instance, *args, **kwargs):
    return instance, func(*args, **kwargs)


def test_standalone_func():

    @my_wrapper
    def func(a, b):
        return a, b

    assert func(1, b=2) == (None, (1, 2))


def test_instance_func():

    class Tmp:

        @my_wrapper
        def func(self, a, b):
            return a, b

    tmp = Tmp()
    assert tmp.func(1, b=2) == (tmp, (1, 2))


def test_class_func():

    class Tmp:

        @my_wrapper
        def func(self, a, b):
            return a, b

    tmp = Tmp()
    assert Tmp.func(tmp, 1, b=2) == (tmp, (1, 2))


def test_class_method():

    class Tmp:

        @my_wrapper
        @classmethod
        def func(cls, a, b):
            return a, b

    assert Tmp.func(1, b=2) == (Tmp, (1, 2))

def test_static_method():

    class Tmp:

        @my_wrapper
        @staticmethod
        def func(a, b):
            return a, b

    assert Tmp.func(1, b=2) == (None, (1, 2))
