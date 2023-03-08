import functools


class FuncProxy:
    """
    Class which looks and behaves like what it wraps.
    """

    def __init__(self, func):
        self.func = func
        try:
            self.__name__ = func.__name__
        except AttributeError:
            pass

    @property
    def __class__(self):
        return self.func.__class__

    def __getattr__(self, name):
        return getattr(self.func, name)


class BoundFuncWrapper(FuncProxy):

    def __init__(self, func, instance, wrapper, is_instance_method):
        FuncProxy.__init__(self, func)
        self.instance = instance
        self.wrapper = wrapper
        self.is_instance_method = is_instance_method

    def __call__(self, *args, **kwargs):
        if self.is_instance_method:
            if self.instance is None:
                instance, *args = args
                func = functools.partial(self.func, instance)
                return self.wrapper(func, instance, *args, **kwargs)
            return self.wrapper(self.func, self.instance, *args, **kwargs)
        instance = getattr(self.func, '__self__', None)
        return self.wrapper(self.func, instance, *args, **kwargs)


class FuncWrapper(FuncProxy):

    def __init__(self, func, wrapper):
        FuncProxy.__init__(self, func)
        self.wrapper = wrapper
        self.is_instance_method = not (
            isinstance(func, classmethod) or isinstance(func, staticmethod)
        )

    def __get__(self, instance, owner):
        """
        Class function objects are actually descriptors.
        When you access that function using a dotted attribute path,
        you are invoking the __get__() method to bind the function to the class instance,
        turning it into a bound method of that object.
        So when calling a method of a class, it is not the __call__() method of the original function object that is called,
        but the __call__() method of the temporary bound object that is created as a result of accessing the function.

        This method is aimed to solve the problem where the wrapper is not honouring the descriptor protocol
        and performing binding on the wrapped object in the case of a method on a class.
        Thus wrappers for class methods need also to be descriptors.
        """
        func = self.func.__get__(instance, owner)
        return BoundFuncWrapper(
            func, instance, self.wrapper, self.is_instance_method
        )

    def __call__(self, *args, **kwargs):
        """
        If the wrapper is applied to a normal function, the __call__() method of the wrapper is used.
        """
        return self.wrapper(self.func, None, *args, **kwargs)


def decorator(wrapper):
    """
    A decorator to help us create our own specialized decorators.
    """

    @functools.wraps(wrapper)
    def _decorator(func):
        return FuncWrapper(func, wrapper)

    return _decorator
