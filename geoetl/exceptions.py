"""Exception hierarchy for geoetl."""


class GeoETLError(Exception):
    """Base exception for all geoetl errors."""


class RasterValidationError(GeoETLError):
    """Raster failed validation checks."""


class COGCreationError(GeoETLError):
    """COG creation failed."""


class VectorValidationError(GeoETLError):
    """Vector data failed geometry validation."""


class UnsupportedFormatError(GeoETLError):
    """File format not supported."""


class StorageError(GeoETLError):
    """Storage tier operation failed."""
