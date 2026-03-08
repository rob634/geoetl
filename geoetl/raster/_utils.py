"""Internal raster utilities."""

import numpy as np


def nodata_mask(data: np.ndarray, nodata_value: float | None) -> np.ndarray:
    """Boolean mask: True where data equals nodata, NaN-aware.

    Handles three cases:
      - nodata_value is None → all-False mask
      - nodata_value is NaN → np.isnan(data)
      - otherwise → np.isclose(data, nodata_value)
    """
    if nodata_value is None:
        return np.zeros(data.shape, dtype=bool)
    if np.isnan(nodata_value):
        return np.isnan(data)
    return np.isclose(data, nodata_value)
