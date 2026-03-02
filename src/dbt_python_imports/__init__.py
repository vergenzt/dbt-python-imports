from functools import wraps
from importlib import import_module

import dbt.context.base
from dbt.plugins.manager import dbtPlugin


class DbtPythonImportsPlugin(dbtPlugin):
    """dbt plugin to add `modules.import` method to Jinja context to import arbitrary Python modules"""

    def initialize(self) -> None:
        self._get_context_modules_orig = dbt.context.base.get_context_modules

        @wraps(self._get_context_modules_orig)
        def _wrapper():
            return {
                **self._get_context_modules_orig(),
                "import": self.import_module,
            }

        dbt.context.base.get_context_modules = _wrapper

    @staticmethod
    def import_module(module: str):
        if module.startswith("."):
            raise ValueError("Relative imports not supported by `modules.import`!")
        return import_module(module)


plugins = [DbtPythonImportsPlugin]
