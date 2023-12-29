resource "aws_athena_database" "rappidriver_data" {
  name   = "rappidriver_data"
  bucket = "rodaapp-rappidriverchain"
}

resource "aws_athena_table" "rappi_driver_routes" {
  database_name = aws_athena_database.rappidriver_data.name
  name          = "rappi_driver_routes"
  bucket        = "rodaapp-rappidriverchain"

  schema {
    column {
      name = "gpsID"
      type = "string"
    }
    column {
      name = "timestampStart"
      type = "timestamp"
    }
    column {
      name = "timestampEnd"
      type = "timestamp"
    }
    column {
      name = "measuredDistance"
      type = "float"
    }
  }

  partition_keys {
    column {
      name = "date"
      type = "string"
    }
    column {
      name = "source"
      type = "string"
    }
  }

  table_type = "EXTERNAL_TABLE"
  external_location = "s3://rodaapp-rappidriverchain/rappi_driver_routes/"

  serde_parameters = {
    "serialization.format" = ","
    "field.delim"          = ","
  }

  input_format = "org.apache.hadoop.mapred.TextInputFormat"
  output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
}