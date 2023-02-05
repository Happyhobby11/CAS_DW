from dataclasses import dataclass


@dataclass
class staging_queries:
    PETS_QUERY: str = """
    (SELECT
        p.name AS name,
        p.gender AS gender,
        u.email AS user_email,
        u.year_birth AS user_year_birth,
        u.month_birth AS user_month_birth,
        u.gender AS user_gender,
        u.district AS district,
        f.english_species AS parent_species,
        s.english_species AS species,
        EXTRACT (year FROM p.date_birth) AS year_birth,
        EXTRACT (month FROM p.date_birth) AS month_birth,
        pi.name AS img,
        EXTRACT (year FROM pi.created_at) AS img_year,
        EXTRACT (month FROM pi.created_at) AS img_month
    FROM pets p
    INNER JOIN users u ON p.user_id = u.id
    INNER JOIN species s ON s.id = p.species_id
    INNER JOIN species f ON f.id = s.family_id
    FULL JOIN pets_img pi ON pi.pet_id = p.id)
    pets_info"""
    MAPS_QUERY: str = """
    (SELECT
        mt.chinese_type,
        mt.english_type,
        m.chinese_name,
        m.english_name,
        m.district,
        c.title AS comment_title,
        c.content AS comment_content,
        c.is_thumb,
        EXTRACT (year FROM c.created_at) AS year,
        EXTRACT (month FROM c.created_at) AS month,
        EXTRACT (day FROM c.created_at) AS day,
        EXTRACT (hour FROM c.created_at) AS hour,
        EXTRACT (quarter FROM c.created_at) AS quarter
    FROM maps m
    INNER JOIN map_type mt ON mt.id = m.map_type_id
    FULL JOIN comments c ON c.map_id = m.id)
    maps_info"""
    EVENTS_QUERY: str = """
    (SELECT
        u.email AS host_email,
        e.district AS district,
        e.animal_type,
        EXTRACT (year FROM e.created_at) AS year,
        EXTRACT (month FROM e.created_at) AS month,
        EXTRACT (day FROM e.created_at) AS day,
        EXTRACT (hour FROM e.created_at) AS hour,
        EXTRACT (quarter FROM e.created_at) AS quarter
    FROM events e
    INNER JOIN users u ON u.id = e.host_id)
    events_info"""
    EXP_QUERY: str = """
    (SELECT
        u.email AS poster_email,
        e.title,
        EXTRACT (year FROM e.created_at) AS year,
        EXTRACT (month FROM e.created_at) AS month,
        EXTRACT (day FROM e.created_at) AS day,
        EXTRACT (hour FROM e.created_at) AS hour,
        EXTRACT (quarter FROM e.created_at) AS quarter
    FROM experience e
    INNER JOIN users u on u.id = e.poster_id)
    exp_info"""
    EXP_LIKE_QUERY: str = """
    (SELECT
        e.title AS exp_title,
        u.email AS user_email,
        el.thumb_status,
        EXTRACT (year FROM el.created_at) AS year,
        EXTRACT (month FROM el.created_at) AS month,
        EXTRACT (day FROM el.created_at) AS day,
        EXTRACT (hour FROM el.created_at) AS hour,
        EXTRACT (quarter FROM el.created_at) AS quarter
    FROM experience_like el
    INNER JOIN users u ON u.id = el.user_id
    INNER JOIN experience e ON e.id = el.exp_id)
    exp_like_info"""


qy = staging_queries()


class select_queries:
    PETS_QUERY: str = "name,gender,user_email,user_year_birth,user_month_birth,user_gender,district,parent_species,species,year_birth,month_birth,img,img_year,img_month"
    MAPS_QUERY: str = "chinese_typeenglish_type,chinese_name,english_name,district,comment_title,comment_content,is_thumb,year,month,day,hour,quarter"
    EVENTS_QUERY: str = "host_email,animal_type,district,year,month,day,hour,quarter"
    EXP_QUERY: str = "poster_email,title,year,month,day,hour,quarter"
    EXP_LIKE_QUERY: str = (
        "exp_title,user_email,thumb_status,year,month,day,hour,quarter"
    )


sq = select_queries()

sqtables = [
    sq.PETS_QUERY,
    sq.MAPS_QUERY,
    sq.EVENTS_QUERY,
    sq.EXP_QUERY,
    sq.EXP_LIKE_QUERY,
]
