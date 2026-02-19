from typing import Any


class SingletonMeta(type):
    """
    A metaclass that creates a singleton instance of a class.
    """

    _instances: dict[type, Any] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


def singleton(cls: type[Any]) -> type[Any]:
    """
    Decorator to make a class a singleton by setting its metaclass to SingletonMeta.

    Args:
        cls (Type[Any]): The class to be decorated.

    Returns:
        Type[Any]: The singleton class with SingletonMeta as its metaclass.

    Limitations:
        - **Pickling Not Supported:** Singleton instances created with this decorator
          cannot be pickled. Attempting to pickle such an instance will result in a
          `TypeError` or `AttributeError`. If pickling is required, consider implementing
          custom pickling methods or using a different singleton pattern.
        - **Incompatible with Custom Metaclasses:** The singleton decorator cannot be
          applied to classes that already have a custom metaclass. Doing so will raise a
          `TypeError` due to metaclass conflicts. To use the singleton pattern with such
          classes, you'll need to implement the singleton behavior manually or adjust
          your class design.
    """

    # Create a new class with SingletonMeta as its metaclass
    class SingletonClass(cls, metaclass=SingletonMeta):
        pass

    # Preserve class metadata
    SingletonClass.__name__ = cls.__name__
    SingletonClass.__doc__ = cls.__doc__
    SingletonClass.__module__ = cls.__module__
    SingletonClass.__annotations__ = getattr(cls, "__annotations__", {})

    return SingletonClass
