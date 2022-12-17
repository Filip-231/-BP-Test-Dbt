SELECT * FROM {{ source("DBT", "pipeline_initialization") }}
WHERE `StructField`.`Name`= "Wrong Name"
