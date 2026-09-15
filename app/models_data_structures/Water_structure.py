from dataclasses import dataclass,asdict


@dataclass
class WaterLevel:
    tank_id: str
    water_level_precentage: int
    
@dataclass
class WaterRefillingProgres:
    tank_id: str
    water_level_precentage: int

@dataclass
class WaterDroppingProgres:
    tank_id: str
    water_level_precentage: int
