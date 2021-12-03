from config_vars import *


create_dim_airports = f"""
    SELECT DISTINCT
      SUBSTR(iso_region, 4,2) AS state_id,
      name                    AS airport_name,
      iata_code               AS iata_code,
      local_code              AS local_code,
      coordinates             AS coordinates,
      elevation_ft            AS elevation_ft
    FROM
      `{ project_id }.{ staging_dataset }.airport_codes`
    WHERE iso_country = 'US' AND type != 'closed'
"""

create_dim_cities = f"""
    SELECT DISTINCT
      City                                                             AS city_name,
      State                                                            AS state_name,
      State_Code                                                       AS STATE_ID,
      Median_Age                                                       AS median_age,
      Male_Population                                                  AS male_population,
      Female_Population                                                AS female_population,
      Total_Population                                                 AS total_population,
      Number_of_Veterans                                               AS number_of_veterans,
      Foreign_born                                                     AS foreign_born,
      Average_Household_Size                                           AS avg_household_size,
      AVG(IF(RACE = 'White', COUNT, NULL))                             AS white_population,
      AVG(IF(RACE = 'Black or African-American', COUNT, NULL))         AS black_population,
      AVG(IF(RACE = 'Asian', COUNT, NULL))                             AS asian_population,
      AVG(IF(RACE = 'Hispanic or Latino', COUNT, NULL))                AS latino_population,
      AVG(IF(RACE = 'American Indian and Alaska Native', COUNT, NULL)) AS native_population
    FROM
      `{ project_id }.{ staging_dataset }.us_cities`
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10
"""

create_dim_time = f"""
    SELECT
      arrival_date                         AS date,
      EXTRACT(DAY FROM arrival_date)       AS day,
      EXTRACT(MONTH FROM arrival_date)     AS month,
      EXTRACT(YEAR FROM arrival_date)      AS year,
      EXTRACT(QUARTER FROM arrival_date)   AS quarter,
      EXTRACT(DAYOFWEEK FROM arrival_date) AS dayofweek,
      EXTRACT(WEEK FROM arrival_date)      AS weekofyear
    FROM (
      SELECT
        DISTINCT arrival_date
      FROM
        `{ project_id }.{ final_dataset }.facts_immigration`
        
      UNION DISTINCT
      
      SELECT
        DISTINCT departure_date
      FROM
        `{ project_id }.{ final_dataset }.facts_immigration`)
    
     WHERE arrival_date IS NOT NULL
"""

create_dim_weather = f"""
    SELECT DISTINCT
      EXTRACT(year FROM dt) AS year,
      city,
      country,
      latitude,
      longitude,
      AVG(AverageTemperature) avg_temp,
      AVG(AverageTemperatureUncertainty) avg_temp_uncert
    FROM
      `{ project_id }.{ staging_dataset }.temperature_by_city`
    WHERE
      country = 'United States'
      AND EXTRACT(year FROM dt) = (
                          SELECT
                            MAX(EXTRACT(year FROM dt))
                          FROM
                            `{ project_id }.{ staging_dataset }.temperature_by_city`)
    GROUP BY 1, 2, 3, 4, 5
"""

create_facts_immigration = f"""
    SELECT DISTINCT
      cicid,
      CAST(i94yr AS NUMERIC) AS year,
      CAST(i94mon AS NUMERIC) AS month,
      country_name AS country_origin,
      port_name,
      DATE_ADD('1960-1-1', INTERVAL CAST(arrdate AS INT64) DAY) arrival_date,
      CASE
        WHEN i94mode = 1 THEN 'Air'
        WHEN i94mode = 2 THEN 'Sea'
        WHEN i94mode = 3 THEN 'Land'
        ELSE 'Not reported'
      END AS arrival_mode,
      state_code AS destination_state_code,
      state_name AS destination_state,
      DATE_ADD('1960-1-1', INTERVAL CAST(depdate AS INT64) DAY) AS departure_date,
      CAST(i94bir AS numeric) AS age,
      CASE
        WHEN I94VISA = 1 THEN 'Business'
        WHEN i94visa = 2 THEN 'Pleasure'
        WHEN i94visa = 3 THEN 'Student'
        ELSE 'Other'
      END AS visa_category,
      matflag AS match_flag,
      CAST(biryear AS numeric) AS birth_year,
      CASE
        WHEN gender = 'F' THEN 'FEMALE'
        WHEN GENDER = 'M' THEN 'MALE'
        ELSE 'UNKNOWN'
      END AS gender,
      insnum AS ins_number,
      airline,
      CAST(admnum AS numeric) AS admission_number,
      fltno AS flight_number,
      visatype AS  visa_type
    FROM
      `{ project_id }.{ staging_dataset }.immigration_data` id
    LEFT JOIN
      `{ project_id }.{ final_dataset }.dim_countries` dc
    ON
      dc.country_code = CAST(I94RES AS STRING)
    LEFT JOIN
      `{ project_id }.{ final_dataset }.dim_ports` dp
    ON
      dp.port_code = i94port
    LEFT JOIN
      `{ project_id }.{ final_dataset }.dim_us_states` ds
    ON
      ds.state_code = i94addr
"""
