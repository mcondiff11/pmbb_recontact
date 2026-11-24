-- =====================================================================
-- SWITCHBOARD -> RECONTACT BACKFILL (one-time)
-- Puts every Switchboard text ever sent into recontact as a contact:
--   one row per text (one text per appointment, so one row per CSN),
--   contact_type 9, staff_member 'switchboard', inserted = time of this run,
--   notes = 'Yes' / 'No' / 'No response'.
-- Source: prepared.switchboard.biobank_responses (uid = EMPI;
--         response 1 = Yes, 2 = No, NULL = no response).
-- Safe to re-run: a CSN already in recontact is not inserted again; its notes
-- are only updated if the response has changed (NULL -> 1 or 2).
-- Run as a SQL task / SQL editor: no %sql line.
-- =====================================================================


-- ---------------------------------------------------------------------
-- A. PREVIEW (optional): what will be inserted, and any rows that will be skipped
-- ---------------------------------------------------------------------
-- SELECT
--     count(*)                                              AS texts,
--     count(DISTINCT csn)                                   AS distinct_csns,
--     count_if(csn IS NULL)                                 AS skipped_no_csn,
--     count_if(try_cast(uid AS BIGINT) IS NULL)             AS skipped_bad_uid,
--     count_if(response = 1)                                AS yes,
--     count_if(response = 2)                                AS no,
--     count_if(response IS NULL)                            AS no_response
-- FROM prepared.switchboard.biobank_responses;


-- ---------------------------------------------------------------------
-- B. BACKFILL
-- ---------------------------------------------------------------------
MERGE INTO biobank_analytics.pmbb_saliva.recontact t
USING (
    SELECT
        csn,
        try_cast(uid AS BIGINT) AS empi_id,
        CASE response WHEN 1 THEN 'Yes' WHEN 2 THEN 'No' ELSE 'No response' END AS notes
    FROM prepared.switchboard.biobank_responses
    WHERE csn IS NOT NULL
      AND try_cast(uid AS BIGINT) IS NOT NULL
    -- safety net in case a CSN ever appears twice: prefer Yes, then No, then no response
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY csn
        ORDER BY CASE response WHEN 1 THEN 0 WHEN 2 THEN 1 ELSE 2 END
    ) = 1
) s
ON t.contact_type = 9 AND t.switchboard_csn = s.csn
WHEN MATCHED AND t.notes = 'No response' AND s.notes <> 'No response' THEN UPDATE SET
    t.notes = s.notes
WHEN NOT MATCHED THEN INSERT (empi_id, contact_type, inserted, notes, staff_member, switchboard_csn)
    VALUES (s.empi_id, 9, current_timestamp(), s.notes, 'switchboard', s.csn);


-- ---------------------------------------------------------------------
-- C. CHECK (optional): counts should match the preview's distinct_csns / yes / no / no_response
-- ---------------------------------------------------------------------
-- SELECT notes, count(*) AS n
-- FROM biobank_analytics.pmbb_saliva.recontact
-- WHERE contact_type = 9
-- GROUP BY notes;
