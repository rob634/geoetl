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


class PipelineError(GeoETLError):
    """Pipeline orchestration failed."""


class ZonalStatsError(GeoETLError):
    """Zonal statistics computation failed."""


class H3Error(GeoETLError):
    """H3 hexagonal grid operation failed."""
