import os

CHARS_NULL = [",", ".", "'", "-", "_", "", "<NA>", "N/A"]
ENGINE_AVAILABLE = ["mysql", "postgres", "oracle", "athena"]
EXTENSION_AVAILABLE = ["xlsx", "csv"]
VALOR_NO_REPRESENTATIVO = os.getenv("VALOR_NO_REPRESENTATIVO", "**!!**")
NOMBRES_GENERALES = [
    os.getenv("COMPLETITUD_GENERAL", "completitud_row"),
    os.getenv("EXACTITUD_GENERAL", "exactitud_row")]
