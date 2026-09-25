"""CI-TOOLING: src.domain (and the src.api.schemas shim) re-export exactly the public
names defined in src/domain/models.py and errors.py, not the helpers those modules
import for themselves (BaseModel, Field, datetime, urlparse, ...). A new model that is
not added to src/domain/__init__.py fails here instead of as an ImportError elsewhere."""
import ast
from pathlib import Path

import src.api.schemas as schemas
import src.domain as domain

DOMAIN_DIR = Path(domain.__file__).parent


def _defined_public_names(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(target.id for target in node.targets if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return {name for name in names if not name.startswith("_")}


def test_domain_exports_every_public_model_and_error():
    expected = _defined_public_names(DOMAIN_DIR / "models.py") | _defined_public_names(DOMAIN_DIR / "errors.py")

    assert sorted(domain.__all__) == sorted(expected)


def test_only_the_exported_names_are_public():
    submodules = {"errors", "models"}  # package attributes set by importing them
    for module in (domain, schemas):
        public = {name for name in vars(module) if not name.startswith("_")} - submodules
        assert public == set(domain.__all__), module.__name__


def test_the_schemas_shim_re_exports_the_same_objects():
    assert schemas.__all__ == domain.__all__
    assert [name for name in domain.__all__ if getattr(schemas, name) is not getattr(domain, name)] == []
