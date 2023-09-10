from importlib.metadata import PackageNotFoundError, version

from .client import AccountClient, AdminClient, BaseClient, UserClient
from .serde import column

try:
    __version__ = version("plantable")
except PackageNotFoundError:
    # package is not installed
    pass
