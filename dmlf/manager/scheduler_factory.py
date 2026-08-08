import importlib
from typing import Type
from dmlf.manager.schedulers.base import BaseScheduler

_DEFAULT_PLUGIN = "dmlf.manager.schedulers.builtin:PriorityBinPackScheduler"

def load_scheduler_class(dotted_path: str | None = None) -> Type[BaseScheduler]:
    """
    Import a scheduler class from a ``module:Class`` string.
    Falls back to the built‑in PriorityBinPackScheduler.
    """
    path = dotted_path or _DEFAULT_PLUGIN
    module_name, class_name = path.rsplit(":", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    if not issubclass(cls, BaseScheduler):
        raise TypeError(f"{cls} is not a subclass of BaseScheduler")
    return cls