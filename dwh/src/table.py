from dataclasses import dataclass


@dataclass
class staging_tables:
    PETS_TABLE: str = "staging_pets"
    MAPS_TABLE: str = "staging_maps"
    EVENTS_TABLE: str = "staging_events"
    EXP_TABLE: str = "staging_exp"
    EXP_LIKE_TABLE: str = "staging_exp_like"


class fact_dim:
    DIM_DATE: str = "dim_date"
    DIM_DISTRICT: str = "dim_district"
    DIM_map: str = "dim_map"
    DIM_PARENT_SPECIES: str = "dim_parent_species"
    DIM_SPECIES: str = "dim_species"
    DIM_USER: str = "dim_user"
    FACT_COMMENT: str = "fact_comment"
    FACT_EVENTS: str = "fact_events"
    FACT_EXP: str = "fact_exp"
    FACT_EXP_LIKE: str = "fact_exp_like"
    FACT_PETS: str = "fact_pets"
    FACT_PETS_IMG: str = "fact_pets_img"


bgtables = [
    fact_dim.DIM_DATE,
    fact_dim.DIM_DISTRICT,
    fact_dim.DIM_map,
    fact_dim.DIM_PARENT_SPECIES,
    fact_dim.DIM_SPECIES,
    fact_dim.DIM_USER,
    fact_dim.FACT_COMMENT,
    fact_dim.FACT_EVENTS,
    fact_dim.FACT_EXP,
    fact_dim.FACT_EXP_LIKE,
    fact_dim.FACT_PETS,
    fact_dim.FACT_PETS_IMG,
]

tb = staging_tables()
