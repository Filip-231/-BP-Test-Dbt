SELECT *
FROM (  -- noqa: L042
  SELECT
    `Name`,
    `Version`,
    `EndDate`,
    STRUCT (
      `Id`,
      `CurrentDateTime`,
      COALESCE(`Event`.`EndDateTime`, CURRENT_DATETIME()) - `CurrentDateTime` AS `PipelineExecutionDuration`,
      `Name` AS `LatestPipeline`,
      `Event`.`Name` AS `LatestEventName`,
      `Event`.`EndDateTime` AS `LatestEventEndDateTime`,
      `Event`.`Traceback` AS `LatestTraceback`,
      CASE`Event`.`Status`
        WHEN "start" THEN IF(
          DATETIME_DIFF(CURRENT_DATETIME(), `Event`.`StartDateTime`, MINUTE) < 1,
          "in progress",
          "lost"
        )
        WHEN "success" THEN IF(
          `Event`.`Name` = "FINAL_EVENT",
          "success",
          IF(
            DATETIME_DIFF(CURRENT_DATETIME(), `Event`.`StartDateTime`, SECOND) < 100,
            "in progress",
            "lost"
          )
        )
        WHEN "fail" THEN "fail"
      END AS `FinalStatus`
    ) AS `Status`
  FROM {{ ref("int_events") }}
  QUALIFY RANK() OVER `MostRecentExecutionPerInstance` = 1
    AND ROW_NUMBER() OVER `MostRecentEventPerExecution` = 1
  WINDOW
    `MostRecentExecutionPerInstance` AS (
      PARTITION BY `Name`, `Version`, `EndDate`
      ORDER BY `CurrentDateTime` DESC
    ),
    `MostRecentEventPerExecution` AS (
      PARTITION BY `Id`
      ORDER BY `Event`.`StartDateTime` DESC, `Event`.`EndDateTime` DESC
    )
)
