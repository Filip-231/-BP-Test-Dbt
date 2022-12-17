SELECT *
FROM (  -- noqa: L042
  SELECT
    `Id`,
    `Name`,
    `Version`,
    `StartDate`,
    `EndDate`,
    `CurrentDateTime`,
    `Event`,
    CASE `Event`.`Status`
      WHEN "fail" THEN "fail"
      WHEN "success" THEN "success"
      ELSE IF(
        DATETIME_DIFF(CURRENT_DATETIME(), `Event`.`StartDateTime`, SECOND) < 600,
        "progress",
        "lost"
      ) END AS `FinalStatus`,
    FIRST_VALUE(`Event`.`Name`) OVER `MostRecentStatusLogErrorMessagePerExecution` AS `ErrorMessage`,
    COALESCE(
      FIRST_VALUE(`Event`.`EndDateTime`) OVER `MostRecentStatusLogPerExecution`,
      CURRENT_DATETIME()
    ) - `CurrentDateTime` AS `PipelineExecutionDuration`
  FROM {{ ref("int_events") }}
  QUALIFY
    ROW_NUMBER() OVER `MostRecentStatusLogPerEvent` = 1
  WINDOW
    `MostRecentStatusLogPerExecution` AS (
      PARTITION BY `Id`
      ORDER BY `Event`.`StartDateTime` DESC, `Event`.`EndDateTime` DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ),
    `MostRecentStatusLogErrorMessagePerExecution` AS (
      PARTITION BY `Id`
      ORDER BY `Event`.`EndDateTime` DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ),
    `MostRecentStatusLogPerEvent` AS (
      PARTITION BY `Id`, `Event`.`Name`
      ORDER BY `Event`.`StartDateTime` DESC, `Event`.`EndDateTime` DESC
    )
)
  PIVOT( --noqa: PRS
    ANY_VALUE(`FinalStatus`)
    FOR `Event`.`Name` IN ("INITIALIZATION_EVENT","FIRST_EVENT", "MIDDLE_EVENT", "LAST_EVENT")
)
