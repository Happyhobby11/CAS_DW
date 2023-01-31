DROP TABLE IF EXISTS staging_pets;
DROP TABLE IF EXISTS staging_maps;
DROP TABLE IF EXISTS staging_events;
DROP TABLE IF EXISTS staging_exp;
DROP TABLE IF EXISTS staging_exp_like;
DROP TABLE IF EXISTS fact_pets_img;
DROP TABLE IF EXISTS fact_pets;
DROP TABLE IF EXISTS fact_exp_like;
DROP TABLE IF EXISTS fact_exp;
DROP TABLE IF EXISTS fact_events;
DROP TABLE IF EXISTS fact_comment;
DROP TABLE IF EXISTS dim_map;
DROP TABLE IF EXISTS dim_species;
DROP TABLE IF EXISTS dim_parent_species;
DROP TABLE IF EXISTS dim_user;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_district;
CREATE TABLE dim_district(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE dim_date(
    id SERIAL PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    quarter INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_unique_date ON dim_date (year, month, day, hour, quarter);
CREATE TABLE dim_user(
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    year_birth INTEGER NOT NULL,
    month_birth INTEGER NOT NULL,
    gender VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE dim_parent_species(
    id SERIAL PRIMARY KEY,
    species VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE dim_species(
    id SERIAL PRIMARY KEY,
    species VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE dim_map(
    id SERIAL PRIMARY KEY,
    chinese_type VARCHAR(255) NOT NULL,
    english_type VARCHAR(255) NOT NULL,
    chinese_name VARCHAR(255) NOT NULL,
    english_name VARCHAR(255) NOT NULL,
    district_id INTEGER,
    FOREIGN KEY (district_id) REFERENCES dim_district(id),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_unique_map ON dim_map(chinese_name, english_name);
CREATE TABLE fact_comment(
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    content text,
    is_thumb BOOLEAN,
    date_id INTEGER,
    FOREIGN KEY (date_id) REFERENCES dim_date(id),
    map_id INTEGER,
    FOREIGN KEY (map_id) REFERENCES dim_map(id)
);
CREATE TABLE fact_events(
    id SERIAL PRIMARY KEY,
    host_email VARCHAR(255) NOT NULL,
    animal_type VARCHAR(255) NOT NULL,
    date_id INTEGER,
    FOREIGN KEY (date_id) REFERENCES dim_date(id),
    district_id INTEGER,
    FOREIGN KEY (district_id) REFERENCES dim_district(id)
);
CREATE TABLE fact_exp(
    id SERIAL PRIMARY KEY,
    poster_email VARCHAR(255),
    title VARCHAR(255),
    date_id INTEGER,
    FOREIGN KEY (date_id) REFERENCES dim_date(id)
);
CREATE TABLE fact_exp_like(
    id SERIAL PRIMARY KEY,
    user_email VARCHAR(255),
    thumb_status VARCHAR(255),
    exp_title VARCHAR(255),
    date_id INTEGER,
    FOREIGN KEY (date_id) REFERENCES dim_date(id)
);
CREATE UNIQUE INDEX idx_unique_exp_like ON fact_exp_like(user_email, exp_title);
CREATE TABLE fact_pets(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gender VARCHAR(255) NOT NULL,
    user_id INTEGER,
    FOREIGN KEY (user_id) REFERENCES dim_user(id),
    parent_species_id INTEGER,
    FOREIGN KEY (parent_species_id) REFERENCES dim_parent_species(id),
    species_id INTEGER,
    FOREIGN KEY (species_id) REFERENCES dim_species(id),
    date_birth_id INTEGER,
    FOREIGN KEY (date_birth_id) REFERENCES dim_date(id),
    district_id INTEGER,
    FOREIGN KEY (district_id) REFERENCES dim_district(id),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_unique_pet ON fact_pets(name, user_id);
CREATE TABLE fact_pets_img(
    id SERIAL PRIMARY KEY,
    pet_id INTEGER,
    FOREIGN KEY (pet_id) REFERENCES fact_pets(id),
    name VARCHAR(255) UNIQUE,
    date_img_id INTEGER,
    FOREIGN KEY (date_img_id) REFERENCES dim_date(id)
);
CREATE TABLE staging_exp_like(
    id SERIAL PRIMARY KEY,
    exp_title VARCHAR(255),
    user_email VARCHAR(255),
    thumb_status VARCHAR(255),
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    quarter INTEGER
);
CREATE TABLE staging_exp(
    id SERIAL PRIMARY KEY,
    poster_email VARCHAR(255),
    title VARCHAR(255),
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    quarter INTEGER
);
CREATE TABLE staging_events(
    id SERIAL PRIMARY KEY,
    host_email VARCHAR(255),
    animal_type VARCHAR(255),
    district VARCHAR(255),
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    quarter INTEGER
);
CREATE TABLE staging_maps(
    id SERIAL PRIMARY KEY,
    chinese_type VARCHAR(255) NOT NULL,
    english_type VARCHAR(255) NOT NULL,
    chinese_name VARCHAR(255) NOT NULL,
    english_name VARCHAR(255) NOT NULL,
    district VARCHAR(255) NOT NULL,
    comment_title VARCHAR(255),
    comment_content text,
    is_thumb BOOLEAN,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    quarter INTEGER
);
CREATE TABLE staging_pets(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gender VARCHAR(255) NOT NULL,
    user_email VARCHAR(255) NOT NULL,
    user_year_birth INTEGER NOT NULL,
    user_month_birth INTEGER NOT NULL,
    user_gender VARCHAR(255) NOT NULL,
    district VARCHAR(255) NOT NULL,
    parent_species VARCHAR(255) NOT NULL,
    species VARCHAR(255) NOT NULL,
    year_birth INTEGER,
    month_birth INTEGER,
    img VARCHAR(255),
    img_year INTEGER,
    img_month INTEGER
);
-----triggers-----
-----exp-----
CREATE OR REPLACE FUNCTION insert_exp() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE dim_date_id INTEGER;
BEGIN
INSERT INTO dim_date (year, month, day, hour, quarter)
VALUES (
        NEW.year,
        NEW.month,
        NEW.day,
        NEW.hour,
        NEW.quarter
    ) ON CONFLICT (year, month, day, hour, quarter) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_date_id;
INSERT INTO fact_exp (
        poster_email,
        title,
        date_id
    )
VALUES (
        NEW.poster_email,
        NEW.title,
        dim_date_id
    );
RETURN NEW;
END;
$$;
CREATE TRIGGER trigger_insert_fact_exp
AFTER
INSERT ON staging_exp FOR EACH ROW EXECUTE PROCEDURE insert_exp();
-----exp_like-----
CREATE OR REPLACE FUNCTION insert_exp_like() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE dim_date_id INTEGER;
BEGIN
INSERT INTO dim_date (year, month, day, hour, quarter)
VALUES (
        NEW.year,
        NEW.month,
        NEW.day,
        NEW.hour,
        NEW.quarter
    ) ON CONFLICT (year, month, day, hour, quarter) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_date_id;
INSERT INTO fact_exp_like(
        user_email,
        thumb_status,
        exp_title,
        date_id
    )
VALUES (
        NEW.user_email,
        NEW.thumb_status,
        NEW.exp_title,
        dim_date_id
    );
RETURN NEW;
END;
$$;
CREATE TRIGGER trigger_insert_fact_exp_like
AFTER
INSERT ON staging_exp_like FOR EACH ROW EXECUTE PROCEDURE insert_exp_like();
-----events-----
CREATE OR REPLACE FUNCTION insert_events() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE dim_date_id INTEGER;
dim_district_id INTEGER;
BEGIN
INSERT INTO dim_date (year, month, day, hour, quarter)
VALUES (
        NEW.year,
        NEW.month,
        NEW.day,
        NEW.hour,
        NEW.quarter
    ) ON CONFLICT (year, month, day, hour, quarter) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_date_id;
INSERT INTO dim_district(name)
VALUES (NEW.district) ON CONFLICT (name) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_district_id;
INSERT INTO fact_events(
        host_email,
        animal_type,
        date_id,
        district_id
    )
VALUES (
        NEW.host_email,
        NEW.animal_type,
        dim_date_id,
        dim_district_id
    );
RETURN NEW;
END;
$$;
CREATE TRIGGER trigger_insert_fact_event
AFTER
INSERT ON staging_events FOR EACH ROW EXECUTE PROCEDURE insert_events();
-----comments-----
CREATE OR REPLACE FUNCTION insert_comments() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE dim_map_id INTEGER;
dim_district_id INTEGER;
dim_date_id INTEGER;
BEGIN IF NOT NEW.comment_title IS NULL THEN
INSERT INTO dim_date (year, month, day, hour, quarter)
VALUES (
        NEW.year,
        NEW.month,
        NEW.day,
        NEW.hour,
        NEW.quarter
    ) ON CONFLICT (year, month, day, hour, quarter) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_date_id;
END IF;
INSERT INTO dim_district(name)
VALUES (NEW.district) ON CONFLICT (name) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_district_id;
INSERT INTO dim_map (
        chinese_type,
        english_type,
        chinese_name,
        english_name,
        district_id
    )
VALUES (
        NEW.chinese_type,
        NEW.english_type,
        NEW.chinese_name,
        NEW.english_name,
        dim_district_id
    ) ON CONFLICT (chinese_name, english_name) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_map_id;
IF NOT NEW.comment_title IS NULL THEN
INSERT INTO fact_comment (
        title,
        content,
        is_thumb,
        date_id,
        map_id
    )
VALUES (
        NEW.comment_title,
        NEW.comment_content,
        NEW.is_thumb,
        dim_date_id,
        dim_map_id
    );
END IF;
RETURN NEW;
END;
$$;
CREATE TRIGGER trigger_insert_fact_comment
AFTER
INSERT ON staging_maps FOR EACH ROW EXECUTE PROCEDURE insert_comments();
-----pets-----
CREATE OR REPLACE FUNCTION insert_pets() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE dim_user_id INTEGER;
dim_parent_species_id INTEGER;
dim_species_id INTEGER;
dim_date_birth_id INTEGER;
dim_date_img_id INTEGER;
dim_district_id INTEGER;
fact_pet_id INTEGER;
BEGIN
INSERT INTO dim_parent_species (species)
VALUES (NEW.parent_species) ON CONFLICT (species) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_parent_species_id;
INSERT INTO dim_species (species)
VALUES (NEW.species) ON CONFLICT (species) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_species_id;
INSERT INTO dim_user (
        email,
        year_birth,
        month_birth,
        gender
    )
VALUES (
        NEW.user_email,
        NEW.user_year_birth,
        NEW.user_month_birth,
        NEW.user_gender
    ) ON CONFLICT (email) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_user_id;
INSERT INTO dim_date (year, month, day, hour, quarter)
VALUES (
        NEW.year_birth,
        NEW.month_birth,
        NULL,
        NULL,
        NULL
    ) ON CONFLICT (year, month, day, hour, quarter) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_date_birth_id;
IF NOT NEW.img IS NULL THEN
INSERT INTO dim_date (year, month, day, hour, quarter)
VALUES (
        NEW.img_year,
        NEW.img_month,
        NULL,
        NULL,
        NULL
    ) ON CONFLICT (year, month, day, hour, quarter) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_date_img_id;
END IF;
INSERT INTO dim_district(name)
VALUES (NEW.district) ON CONFLICT (name) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO dim_district_id;
INSERT INTO fact_pets (
        name,
        gender,
        user_id,
        parent_species_id,
        species_id,
        date_birth_id,
        district_id
    )
VALUES (
        NEW.name,
        NEW.gender,
        dim_user_id,
        dim_parent_species_id,
        dim_species_id,
        dim_date_birth_id,
        dim_district_id
    ) ON CONFLICT (name, user_id) DO
UPDATE
SET updated_at = NOW()
RETURNING id INTO fact_pet_id;
IF NOT NEW.img IS NULL THEN
INSERT INTO fact_pets_img (pet_id, name, date_img_id)
VALUES (fact_pet_id, NEW.img, dim_date_img_id);
END IF;
RETURN NEW;
END;
$$;
CREATE TRIGGER trigger_insert_fact_pets
AFTER
INSERT ON staging_pets FOR EACH ROW EXECUTE PROCEDURE insert_pets();