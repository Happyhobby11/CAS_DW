from dataclasses import dataclass


@dataclass
class staging_tables:
    PETS_TABLE: str = "staging_pets"
    MAPS_TABLE: str = "staging_maps"
    EVENTS_TABLE: str = "staging_events"
    EXP_TABLE: str = "staging_exp"
    EXP_LIKE_TABLE: str = "staging_exp_like"
class fact_dim:
    




tb = staging_tables()
