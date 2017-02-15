SELECT_WEBSITE = """
SELECT *
FROM (
       SELECT
         bg.id,
         bg.website,
         1 AS type
       FROM games bg
       WHERE bg.deleted = FALSE
             AND bg.domain IS NOT NULL
             AND bg.website IS NOT NULL
             AND bg.website NOT IN (\'\', \'-\')
             AND NOT exists(
           SELECT 1
           FROM socials t
           WHERE t.url = bg.website
       )

       UNION

       SELECT
         bo.id,
         bo.website,
         2 AS type
       FROM orgs bo
       WHERE bo.deleted = FALSE
             AND bo.domain IS NOT NULL
             AND bo.website IS NOT NULL
             AND bo.website NOT IN (\'\', \'-\')
             AND NOT exists(
           SELECT 1
           FROM socials t
           WHERE t.url = bo.website
       )
     ) t
ORDER BY t.type ASC
"""

INSERT_DATA = """
insert into socials (id, url, domain, twitters, error, cdate, mdate)
values (nextval('socials_id_seq'), $1, $2, $3, $4, current_timestamp, current_timestamp);
"""
