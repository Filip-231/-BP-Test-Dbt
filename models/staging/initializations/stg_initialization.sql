{{ config(
    pre_hook= "INSERT INTO
                  {{ source('DBT', 'pipeline_initialization') }}
                VALUES
                  ( STRUCT( CAST(ROUND(RAND() * 1000000) AS STRING),
                      --Id
                      'Name',
                      -- Name
                      '1234567',
                      --Version
                      PARSE_DATE('%F', '2022-05-02'),
                      --StartDate
                      PARSE_DATE('%F', '2022-05-03'),
                      --EndDate
                      PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', '2022-04-26T08:22:27.120Z'),
                      --CurrentDateTime,
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
  `StructField`.`CurrentDatetime` AS `CurrentDatetime`,
  `StructField`.`Name` AS `ExecutionName`,
  `StructField`.`Version` AS `Version`,
  `StructField`.`StartDate` AS `StartDate`,
  `StructField`.`EndDate` AS `EndDate`
FROM {{ source("DBT", "pipeline_initialization") }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY `Id` ORDER BY `CurrentDatetime`) = 1
