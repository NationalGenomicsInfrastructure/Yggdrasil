import unittest
from typing import Generic, TypeVar

T = TypeVar("T")

from yggdrasil.daemon.singleton import SingletonMeta, singleton


class TestSingletonDecorator(unittest.TestCase):

    def test_singleton_basic(self):
        @singleton
        class MyClass:
            pass

        instance1 = MyClass()
        instance2 = MyClass()

        self.assertIs(instance1, instance2)
        self.assertEqual(id(instance1), id(instance2))

    def test_singleton_with_args(self):
        @singleton
        class MyClass:
            def __init__(self, value):
                self.value = value

        instance1 = MyClass(10)
        instance2 = MyClass(20)

        self.assertIs(instance1, instance2)
        self.assertEqual(instance1.value, 10)
        self.assertEqual(instance2.value, 10)

    def test_singleton_different_classes(self):
        @singleton
        class ClassA:
            pass

        @singleton
        class ClassB:
            pass

        instance_a1 = ClassA()
        instance_a2 = ClassA()
        instance_b1 = ClassB()
        instance_b2 = ClassB()

        self.assertIs(instance_a1, instance_a2)
        self.assertIs(instance_b1, instance_b2)
        self.assertIsNot(instance_a1, instance_b1)

    def test_singleton_inheritance(self):
        @singleton
        class BaseClass:
            pass

        class SubClass(BaseClass):
            pass

        base_instance1 = BaseClass()
        base_instance2 = BaseClass()
        sub_instance1 = SubClass()
        sub_instance2 = SubClass()

        self.assertIs(base_instance1, base_instance2)
        self.assertIs(sub_instance1, sub_instance2)
        self.assertIsNot(base_instance1, sub_instance1)

    def test_singleton_with_kwargs(self):
        @singleton
        class MyClass:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        instance1 = MyClass(a=1, b=2)
        instance2 = MyClass(a=3, b=4)

        self.assertIs(instance1, instance2)
        self.assertEqual(instance1.kwargs, {"a": 1, "b": 2})
        self.assertEqual(instance2.kwargs, {"a": 1, "b": 2})

    def test_singleton_reset_instance(self):
        @singleton
        class MyClass:
            pass

        instance1 = MyClass()
        instance2 = MyClass()

        self.assertIs(instance1, instance2)

        # Reset the singleton instance
        SingletonMeta._instances.pop(MyClass, None)
        instance3 = MyClass()

        self.assertIsNot(instance1, instance3)

    def test_singleton_thread_safety(self):
        # Note: The current singleton implementation is not thread-safe.
        # This test demonstrates that, and in a real-world scenario,
        # you should use threading locks to make it thread-safe.

        import threading

        @singleton
        class MyClass:
            def __init__(self, value):
                self.value = value

        instances = []

        def create_instance(value):
            instances.append(MyClass(value))

        threads = []
        for i in range(10):
            thread = threading.Thread(target=create_instance, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Check that all instances are the same
        for instance in instances:
            self.assertIs(instance, instances[0])

    def test_singleton_with_multiple_instances(self):
        # Ensure that singleton instances are maintained separately for different classes
        @singleton
        class MyClassA:
            def __init__(self, value):
                self.value = value

        @singleton
        class MyClassB:
            def __init__(self, value):
                self.value = value

        instance_a1 = MyClassA(1)
        instance_a2 = MyClassA(2)
        instance_b1 = MyClassB(3)
        instance_b2 = MyClassB(4)

        self.assertIs(instance_a1, instance_a2)
        self.assertIs(instance_b1, instance_b2)
        self.assertIsNot(instance_a1, instance_b1)
        self.assertEqual(instance_a1.value, 1)
        self.assertEqual(instance_b1.value, 3)

    def test_singleton_with_classmethod(self):
        @singleton
        class MyClass:
            @classmethod
            def cls_method(cls):
                return "cls_method called"

        instance1 = MyClass()
        instance2 = MyClass()

        self.assertIs(instance1, instance2)
        self.assertEqual(MyClass.cls_method(), "cls_method called")

    def test_singleton_with_staticmethod(self):
        @singleton
        class MyClass:
            @staticmethod
            def static_method():
                return "static_method called"

        instance1 = MyClass()
        instance2 = MyClass()

        self.assertIs(instance1, instance2)
        self.assertEqual(MyClass.static_method(), "static_method called")

    def test_singleton_with_property(self):
        @singleton
        class MyClass:
            def __init__(self, value):
                self._value = value

            @property
            def value(self):
                return self._value

        instance1 = MyClass(10)
        instance2 = MyClass(20)

        self.assertIs(instance1, instance2)
        self.assertEqual(instance1.value, 10)

    def test_singleton_decorator_without_parentheses(self):
        # Ensure that the singleton decorator can be used without parentheses
        @singleton
        class MyClass:
            pass

        instance1 = MyClass()
        instance2 = MyClass()

        self.assertIs(instance1, instance2)

    def test_singleton_repr(self):
        @singleton
        class MyClass:
            pass

        instance = MyClass()
        self.assertEqual(repr(instance), repr(MyClass()))

    def test_singleton_str(self):
        @singleton
        class MyClass:
            pass

        instance = MyClass()
        self.assertEqual(str(instance), str(MyClass()))

    def test_singleton_isinstance(self):
        @singleton
        class MyClass:
            pass

        instance = MyClass()
        self.assertIsInstance(instance, MyClass)

    def test_singleton_pickle_not_supported(self):
        """Test that pickling a singleton instance is not supported
        and raises an exception.
        """
        import pickle

        @singleton
        class MyClass:
            def __init__(self, value):
                self.value = value

        instance = MyClass(10)

        with self.assertRaises((TypeError, AttributeError, pickle.PicklingError)):
            pickle.dumps(instance)

    def test_singleton_subclassing_singleton(self):
        @singleton
        class BaseClass:
            pass

        @singleton
        class SubClass(BaseClass):
            pass

        base_instance = BaseClass()
        sub_instance = SubClass()

        self.assertIsNot(base_instance, sub_instance)
        self.assertIsInstance(sub_instance, SubClass)
        self.assertIsInstance(sub_instance, BaseClass)

    def test_singleton_metaclass_conflict(self):
        """Test that applying the singleton decorator toa class
        with a custom metaclass raises a TypeError.
        """

        class Meta(type):
            pass

        with self.assertRaises(TypeError):

            @singleton
            class MyClass(metaclass=Meta):
                pass

    def test_singleton_with_decorated_class(self):
        def decorator(cls):
            cls.decorated = True
            return cls

        @singleton
        @decorator
        class MyClass:
            pass

        instance = MyClass()
        self.assertTrue(hasattr(instance, "decorated"))
        self.assertTrue(instance.decorated)

    def test_singleton_with_exceptions_in_init(self):
        @singleton
        class MyClass:
            def __init__(self, value):
                if value < 0:
                    raise ValueError("Negative value not allowed")
                self.value = value

        with self.assertRaises(ValueError):
            MyClass(-1)

        # Instance should not be created due to exception
        self.assertFalse(MyClass in SingletonMeta._instances)

        # Creating with valid value
        instance = MyClass(10)
        self.assertEqual(instance.value, 10)

    def test_singleton_docstring_preserved(self):
        @singleton
        class MyClass:
            """This is MyClass docstring."""

            pass

        self.assertEqual(MyClass.__doc__, "This is MyClass docstring.")

    def test_singleton_name_preserved(self):
        @singleton
        class MyClass:
            pass

        self.assertEqual(MyClass.__name__, "MyClass")

    def test_singleton_module_preserved(self):
        @singleton
        class MyClass:
            pass

        self.assertEqual(MyClass.__module__, __name__)

    def test_singleton_annotations_preserved(self):
        @singleton
        class MyClass:
            x: int

            def __init__(self, x: int):
                self.x = x

        instance = MyClass(10)
        self.assertEqual(instance.x, 10)
        self.assertEqual(MyClass.__annotations__, {"x": int})

    def test_singleton_with_slots(self):
        @singleton
        class MyClass:
            __slots__ = ["value"]

            def __init__(self, value):
                self.value = value

        instance1 = MyClass(10)
        instance2 = MyClass(20)

        self.assertIs(instance1, instance2)
        self.assertEqual(instance1.value, 10)

    def test_singleton_with_weakref(self):
        import weakref

        @singleton
        class MyClass:
            pass

        instance = MyClass()
        weak_instance = weakref.ref(instance)
        self.assertIs(weak_instance(), instance)

    def test_singleton_with_del(self):
        @singleton
        class MyClass:
            pass

        instance1 = MyClass()
        del instance1

        instance2 = MyClass()
        self.assertIsNotNone(instance2)

    def test_singleton_reset_between_tests(self):
        @singleton
        class MyClass:
            pass

        instance1 = MyClass()
        instance2 = MyClass()
        self.assertIs(instance1, instance2)

        # Reset the instance (for testing purposes)
        SingletonMeta._instances.pop(MyClass, None)

        instance3 = MyClass()
        self.assertIsNot(instance1, instance3)

    def test_singleton_no_args(self):
        @singleton
        class MyClass:
            def __init__(self):
                self.value = 42

        instance = MyClass()
        self.assertEqual(instance.value, 42)

    def test_singleton_calling_class_directly(self):
        @singleton
        class MyClass:
            pass

        instance = MyClass()
        # Since MyClass is a class, calling it directly is the correct way
        direct_instance = MyClass()

        self.assertIs(instance, direct_instance)

    def test_singleton_calling_get_instance_directly(self):
        @singleton
        class MyClass:
            pass

        # Access the get_instance function directly
        get_instance = MyClass
        instance1 = get_instance()
        instance2 = get_instance()

        self.assertIs(instance1, instance2)

    def test_singleton_multiple_arguments(self):
        @singleton
        class MyClass:
            def __init__(self, a, b, c):
                self.total = a + b + c

        instance1 = MyClass(1, 2, 3)
        instance2 = MyClass(4, 5, 6)

        self.assertIs(instance1, instance2)
        self.assertEqual(instance1.total, 6)

    def test_singleton_class_variables(self):
        @singleton
        class MyClass:
            count = 0

            def __init__(self):
                MyClass.count += 1

        instance1 = MyClass()
        instance2 = MyClass()

        self.assertIs(instance1, instance2)
        self.assertEqual(MyClass.count, 1)

    def test_singleton_with_already_existing_instance(self):
        @singleton
        class MyClass:
            pass

        # Manually add an instance to the instances dict
        SingletonMeta._instances[MyClass] = "ExistingInstance"

        instance = MyClass()
        self.assertEqual(instance, "ExistingInstance")

    def test_singleton_with_different_classes_same_name(self):
        @singleton
        class MyClass:  # type: ignore
            pass

        # Define another class with the same name
        @singleton
        class MyClass:  # noqa: F811
            pass

        instance1 = MyClass()
        instance2 = MyClass()

        self.assertIs(instance1, instance2)

    def test_singleton_with_type_var(self):
        @singleton
        class MyClass(Generic[T]):
            def __init__(self, value: T):
                self.value = value

        instance1 = MyClass(10)
        instance2 = MyClass(20)

        self.assertIs(instance1, instance2)
        self.assertEqual(instance1.value, 10)


if __name__ == "__main__":
    unittest.main()
