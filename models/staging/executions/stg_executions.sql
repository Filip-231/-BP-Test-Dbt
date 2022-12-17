{{ config(
    pre_hook= "INSERT INTO
                  {{ source('DBT', 'pipeline_execution') }}
                VALUES
                  ( STRUCT( CAST(ROUND(RAND() * 1000000) AS STRING),
                      --Id
                      'Name',
                      -- Name
                      PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', '2022-04-26T08:22:27.120Z'),
                      --CurrentDateTime,
                      PARSE_DATE('%F', '2022-05-03'),
                      --EndDate
                      STRUCT( 'FINAL_EVENT',
                        --Name
                        'Description text.',
                        --Description
                        CURRENT_DATETIME('UTC'),
                        --StartDateTime
                        CURRENT_DATETIME('UTC'),
                        --EndDateTime
                        'success',
                        --Status
                        '' --Traceback
                        ),
                      JSON '{\"key_1\": \"Data\", \"key_2\": 30}'),
                      CURRENT_TIMESTAMP() --Timestamp
                    );"
) }}

SELECT
  `StructField`.`Id` AS `Id`,
  `StructField`.`Name` AS `ExecutionName`,
  `StructField`.`EndDate` AS `EndDate`,
  `StructField`.`CurrentDatetime` AS `CurrentDatetime`,
  `StructField`.`Event` AS `Event`
FROM {{ source("DBT", "pipeline_execution") }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY `Id` ORDER BY `CurrentDatetime`) = 1
