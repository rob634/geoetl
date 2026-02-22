"""Pydantic models for vector validation."""

from pydantic import BaseModel


class StageStats(BaseModel):
    stage: str
    input_count: int
    output_count: int
    affected: int = 0
    details: str = ""


class ValidationReport(BaseModel):
    stages: list[StageStats] = []
    input_count: int = 0
    output_count: int = 0

    def add(self, stats: StageStats):
        self.stages.append(stats)
        if not self.input_count:
            self.input_count = stats.input_count
        self.output_count = stats.output_count

    @property
    def total_affected(self) -> int:
        return sum(s.affected for s in self.stages)

    @property
    def rows_removed(self) -> int:
        return self.input_count - self.output_count
