WITH `cte_last_initialization` AS (
  SELECT `StructField`.*
  FROM {{ source("DBT", "pipeline_initialization") }}
  QUALIFY ROW_NUMBER() OVER `last_event` = 1
  WINDOW
    `last_event` AS (
      PARTITION BY `StructField`.`Id`
      ORDER BY `StructField`.`Event`.`StartDateTime` DESC, `StructField`.`Event`.`EndDateTime` DESC
    )
),

`cte_last_execution` AS (
  SELECT
    `StructField`.`Id`,
    `StructField`.`Event`,
    `StructField`.`Details`
  FROM {{ source("DBT", "pipeline_execution") }}
  QUALIFY ROW_NUMBER() OVER `last_event` = 1
  WINDOW
    `last_event` AS (
      PARTITION BY `StructField`.`Id`
      ORDER BY `StructField`.`Event`.`StartDateTime` DESC, `StructField`.`Event`.`EndDateTime` DESC
    )
),

`cte_all_executions` AS (
  SELECT
    `last_initialization`.`Id`,
    `last_initialization`.`CurrentDateTime`,
    `last_initialization`.`Name`,
    `last_initialization`.`Version`,
    `last_initialization`.`StartDate`,
    `last_initialization`.`EndDate`,
    CONCAT(
      "Running Pipeline with Id: ",
      `last_initialization`.`Id`,
      "at ",
      `last_initialization`.`StartDate`
    ) AS `Title`,
    JSON_VALUE(`last_initialization`.`Details`.`JsonKey`) AS `JsonValue`,
    COALESCE(`last_execution`.`Event`, `last_initialization`.`Event`) AS `LatestEvent`,
    TO_JSON_STRING(`last_execution`.`Details`, TRUE) AS `RowCounts`
  FROM `cte_last_initialization` AS `last_initialization`
  LEFT JOIN `cte_last_execution` AS `last_execution`
    ON `last_initialization`.`Id` = `last_execution`.`Id`
),


`cte_executions_status` AS (
  SELECT  -- noqa: L034
    CASE `LatestEvent`.`Status`
      WHEN "fail" THEN "fail"
      WHEN "success" THEN IF(
        `LatestEvent`.`Name` = "LAST_EVENT",
        "success",
        IF(
          DATETIME_DIFF(CURRENT_DATETIME(), `LatestEvent`.`EndDateTime`, SECOND) < 300,
          "progress",
          "lost"
        )
      )
      WHEN "start" THEN IF(
        DATETIME_DIFF(CURRENT_DATETIME(), `LatestEvent`.`StartDateTime`, SECOND) < 300,
        "progress",
        "lost"
      )
    END AS `Status`,
    *
  FROM `cte_all_executions`
),

`cte_executions_information` AS (
  SELECT
    *,
    ARRAY_AGG(STRUCT(
      IF(`Status` = "success", GENERATE_DATE_ARRAY(`StartDate`, `EndDate`), []) AS `SuccessfulFetchDates`
    )) OVER `instance_executions_new` AS `MoreRecentSuccessfulFetchDates`,
    IF(
      `Status` = "lost",
      NULL,
      COALESCE(`LatestEvent`.`EndDateTime`, CURRENT_DATETIME()) - `CurrentDateTime`
    ) AS `PipelineDuration`
  FROM `cte_executions_status`
  WINDOW
    `instance_executions_new` AS (
      PARTITION BY `Name`, `Version`
      ORDER BY `CurrentDateTime` DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
)

SELECT
  *,
  {{ parse_id("`Name`") }} AS `parse_id`,
  {{ add_cloud_run_id_column() }},
  {{ add_surrogate_key_column(["`Id`","`Version`"]) }}
FROM `cte_executions_information` AS `executions_information`
