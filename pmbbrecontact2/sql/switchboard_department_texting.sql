-- =====================================================================
-- 1. ONE-TIME SETUP: run once in Databricks SQL before deploying the app
-- =====================================================================

-- Departments that can receive Switchboard texts, managed on the app's Switchboard Settings page.
-- OPT-IN: only departments listed here with texting_enabled = TRUE get texts.
-- A department with no row, or with texting_enabled = FALSE (paused), gets none.
CREATE TABLE IF NOT EXISTS biobank_analytics.pmbb_saliva.department_texting_settings (
    DepartmentEpicId STRING    NOT NULL,
    texting_enabled  BOOLEAN   NOT NULL,
    updated_by       STRING,
    updated_at       TIMESTAMP
);

-- Audit trail: one row per change (who, when, on/paused).
CREATE TABLE IF NOT EXISTS biobank_analytics.pmbb_saliva.department_texting_log (
    DepartmentEpicId STRING    NOT NULL,
    texting_enabled  BOOLEAN   NOT NULL,
    changed_by       STRING,
    changed_at       TIMESTAMP
);

-- Starting state: 1165 and 212 on, 956 paused.
-- Safe to re-run: existing rows are left as they are (change them in the app instead).
MERGE INTO biobank_analytics.pmbb_saliva.department_texting_settings t
USING (
    SELECT * FROM VALUES
        ('1165', TRUE),
        ('212',  TRUE),
        ('956',  FALSE)
    AS v(DepartmentEpicId, texting_enabled)
) s
ON t.DepartmentEpicId = s.DepartmentEpicId
WHEN NOT MATCHED THEN INSERT (DepartmentEpicId, texting_enabled, updated_by, updated_at)
    VALUES (s.DepartmentEpicId, s.texting_enabled, 'initial setup', current_timestamp());

-- The app's service principal needs read/write on both tables, e.g.:
-- GRANT SELECT, MODIFY ON TABLE biobank_analytics.pmbb_saliva.department_texting_settings TO `<app service principal>`;
-- GRANT SELECT, MODIFY ON TABLE biobank_analytics.pmbb_saliva.department_texting_log      TO `<app service principal>`;


-- =====================================================================
-- 2. SWITCHBOARD SEND JOB (daily): queues appointments 4 days out for texting,
--    only in departments switched On in department_texting_settings.
--    Replies are handled by a separate job: see switchboard_results_job.sql.
-- =====================================================================

INSERT OVERWRITE biobank.switchboard.switchboard_appointments (
    HUP_MRN,
    EMPI,
    EncounterEpicCsn,
    AppointmentInstant,
    DepartmentEpicId
)
SELECT
    u.HUP_MRN,
    u.EMPI,
    u.EncounterEpicCsn,
    u.AppointmentInstant,
    u.DepartmentEpicId
FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva u
WHERE u.Switchboard_YN = 'Y'
  AND to_date(u.AppointmentDate) = date_add(current_date(), 4)
  AND EXISTS (
      SELECT 1
      FROM biobank_analytics.pmbb_saliva.department_texting_settings s
      WHERE s.DepartmentEpicId = CAST(u.DepartmentEpicId AS STRING)
        AND s.texting_enabled = TRUE
  );

INSERT INTO biobank_analytics.pmbb_saliva.switchboard_text_attempts (
    HUP_MRN,
    EMPI,
    Patient_name,
    EncounterEpicCsn,
    AppointmentInstant,
    DepartmentEpicId
)
SELECT
    u.HUP_MRN,
    u.EMPI,
    u.Patient_name,
    u.EncounterEpicCsn,
    u.AppointmentInstant,
    u.DepartmentEpicId
FROM biobank_analytics.pmbb_saliva.upcoming_appointments_for_saliva u
WHERE u.Switchboard_YN = 'Y'
  AND to_date(u.AppointmentDate) = date_add(current_date(), 4)
  AND EXISTS (
      SELECT 1
      FROM biobank_analytics.pmbb_saliva.department_texting_settings s
      WHERE s.DepartmentEpicId = CAST(u.DepartmentEpicId AS STRING)
        AND s.texting_enabled = TRUE
  );
