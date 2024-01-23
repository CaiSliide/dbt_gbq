WITH
  yesterday_cohort AS (
  SELECT
    acquisition_date,
    user_pseudo_id,
    last_seen_app_family_name AS product,
    last_seen_app_info_id AS flavour,
    first_seen_app_version_string AS version,
    last_seen_device_model as device_model,
    last_seen_device_os_version AS operating_system,
    CASE
      WHEN activation_date IS NULL OR activation_date > acquisition_date + 1 THEN FALSE
    ELSE
    TRUE
  END
    AS activated
  FROM
    {{ ref("headlines_users") }}
  WHERE
    acquisition_date < CURRENT_DATE() - 1 ),
  minus7_cohort AS (
  SELECT
    acquisition_date,
    user_pseudo_id,
    last_seen_app_family_name AS product,
    last_seen_app_info_id AS flavour,
    first_seen_app_version_string AS version,
    last_seen_device_model as device_model,
    last_seen_device_os_version AS operating_system,
    CASE
      WHEN activation_date IS NULL OR activation_date > acquisition_date + 7 THEN FALSE
    ELSE
    TRUE
  END
    AS activated
  FROM
    {{ ref("headlines_users") }}
  WHERE
    acquisition_date < CURRENT_DATE() - 7 ),
  minus28_cohort AS (
  SELECT
    acquisition_date,
    user_pseudo_id,
    last_seen_app_family_name AS product,
    last_seen_app_info_id AS flavour,
    first_seen_app_version_string AS version,
    last_seen_device_model as device_model,
    last_seen_device_os_version AS operating_system,
    CASE
      WHEN activation_date IS NULL OR activation_date > acquisition_date + 28 THEN FALSE
    ELSE
    TRUE
  END
    AS activated
  FROM
    {{ ref("headlines_users") }}
  WHERE
    acquisition_date < CURRENT_DATE() - 28 )
  -- By Flavour breakdown
SELECT
  acquisition_date AS date,
  product,
  flavour,
  'total' AS version,
  'total' AS device_model,
  'total' AS operating_system,
  '-1 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  yesterday_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  'total' AS version,
  'total' AS device_model,
  'total' AS operating_system,
  '-7 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  minus7_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  'total' AS version,
  'total' AS device_model,
  'total' AS operating_system,
  '-28 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  minus28_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
  -- By Flavour & Version breakdown
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  version,
  'total' AS device_model,
  'total' AS operating_system,
  '-1 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  yesterday_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  version,
  'total' AS device_model,
  'total' AS operating_system,
  '-7 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  minus7_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  version,
  'total' AS device_model,
  'total' AS operating_system,
  '-28 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  minus28_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
  -- By Flavour & Version & Device breakdown
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  version,
  device_model,
  'total' AS operating_system,
  '-1 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  yesterday_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  version,
  device_model,
  'total' AS operating_system,
  '-7 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  minus7_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  version,
  device_model,
  'total' AS operating_system,
  '-28 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  minus28_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
  -- By Flavour & Version & OS breakdown
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  version,
  'total' AS device_model,
  operating_system,
  '-1 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  yesterday_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  version,
  'total' AS device_model,
  operating_system,
  '-7 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  minus7_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system
UNION ALL
SELECT
  acquisition_date AS date,
  product,
  flavour,
  version,
  'total' AS device_model,
  operating_system,
  '-28 day' AS cohort,
  COUNT(DISTINCT user_pseudo_id) AS acqusitions,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) AS activations,
  COUNT(DISTINCT
    CASE
      WHEN activated IS TRUE THEN user_pseudo_id
  END
    ) / COUNT(DISTINCT user_pseudo_id) AS activation_rate
FROM
  minus28_cohort
GROUP BY
  acquisition_date,
  cohort,
  product,
  flavour,
  version,
  device_model,
  operating_system