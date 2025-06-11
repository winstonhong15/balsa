-- SELECT MIN(mc.note) AS production_note
-- FROM company_type AS ct,
--      movie_companies AS mc
-- WHERE ct.kind = 'production companies'
--   AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
--   AND (mc.note LIKE '%(co-production)%'
--        OR mc.note LIKE '%(presents)%')
--   AND ct.id = mc.company_type_id

SELECT min(mc.note) FROM movie_companies AS mc
