"""Overture Maps configuration: release URLs, road classes, building subtypes."""

from enum import Enum

from pydantic import BaseModel, Field


class OvertureTheme(str, Enum):
    TRANSPORTATION = "transportation"
    BUILDINGS = "buildings"
    PLACES = "places"


# Road class hierarchy (most significant → least)
ROAD_CLASSES = [
    "motorway",
    "trunk",
    "primary",
    "secondary",
    "tertiary",
    "unclassified",
    "residential",
    "living_street",
    "service",
    "track",
    "path",
    "cycleway",
    "footway",
    "steps",
]

# Building subtypes we track individually
BUILDING_SUBTYPES = ["residential", "commercial", "industrial"]


class OvertureConfig(BaseModel):
    """Configuration for Overture Maps S3 queries."""

    release: str = Field(default="2026-02-18.0", description="Overture release tag")
    s3_bucket: str = "overturemaps-us-west-2"
    s3_region: str = "us-west-2"
    memory_limit: str = "16GB"
    object_cache: bool = True
    h3_resolution: int = Field(default=5, ge=0, le=15)
    tile_degrees: float = Field(default=5.0, ge=1.0, le=30.0)

    @property
    def s3_base(self) -> str:
        return f"s3://{self.s3_bucket}/release/{self.release}"
