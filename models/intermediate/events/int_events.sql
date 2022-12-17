SELECT
  `executions`.*,
  `pipeline_execution`.`Structfield`.`Name` AS `Name`,
  `pipeline_execution`.`Structfield`.`Event` AS `Event`,
  `pipeline_execution`.`StructField`.`Details`
FROM {{ source("DBT", "pipeline_execution") }} AS `pipeline_execution`
JOIN {{ ref("stg_initialization") }} AS `executions`
  ON `pipeline_execution`.`StructField`.`Id` = `executions`.`Id`


UNION ALL


SELECT
  `executions`.*,
  `pipeline_initialization`.`StructField`.`Name` AS `Name`,
  `pipeline_initialization`.`StructField`.`Event` AS `Event`,
  `pipeline_initialization`.`StructField`.`Details`
FROM {{ source("DBT", "pipeline_initialization") }} AS `pipeline_initialization`
JOIN {{ ref("stg_initialization") }} AS `executions`
  ON `pipeline_initialization`.`StructField`.`Id` = `executions`.`Id`
