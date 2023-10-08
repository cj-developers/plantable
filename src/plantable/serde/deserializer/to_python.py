import logging
from datetime import datetime, date
from typing import Any, List, Union


from ...model import Table
from ..const import DT_FMT, TZ
from .deserializer import TableDeserializer

logger = logging.getLogger(__name__)


################################################################
# Converter
################################################################
class ToPython(TableDeserializer):
    pass
