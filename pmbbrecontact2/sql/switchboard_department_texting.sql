-- =====================================================================
-- 1. ONE-TIME SETUP: run once in Databricks SQL before deploying the app
-- =====================================================================

-- Current on/off state per department. A department with no row is treated as ON,
-- so only departments someone has toggled in the app will appear here.
CREATE TABLE IF NOT EXISTS biobank_analytics.pmbb_saliva.department_texting_settings (
    DepartmentEpicId STRING    NOT NULL,
    texting_enabled  BOOLEAN   NOT NULL,
    updated_by       STRING,
    updated_at       TIMESTAMP
);

-- Audit trail: one row per toggle (who, when, on/off).
CREATE TABLE IF NOT EXISTS biobank_analytics.pmbb_saliva.department_texting_log (
    DepartmentEpicId STRING    NOT NULL,
    texting_enabled  BOOLEAN   NOT NULL,
    changed_by       STRING,
    changed_at       TIMESTAMP
);

-- The app's service principal needs read/write on both tables, e.g.:
-- GRANT SELECT, MODIFY ON TABLE biobank_analytics.pmbb_saliva.department_texting_settings TO `<app service principal>`;
-- GRANT SELECT, MODIFY ON TABLE biobank_analytics.pmbb_saliva.department_texting_log      TO `<app service principal>`;


-- =====================================================================
-- 2. SWITCHBOARD SEND JOB (daily): queues appointments 4 days out for texting.
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
  AND NOT EXISTS (
      SELECT 1
      FROM biobank_analytics.pmbb_saliva.department_texting_settings s
      WHERE s.DepartmentEpicId = CAST(u.DepartmentEpicId AS STRING)
        AND s.texting_enabled = FALSE
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
  AND NOT EXISTS (
      SELECT 1
      FROM biobank_analytics.pmbb_saliva.department_texting_settings s
      WHERE s.DepartmentEpicId = CAST(u.DepartmentEpicId AS STRING)
        AND s.texting_enabled = FALSE
  );
