resource "aws_glue_catalog_database" "rappi_driver_db" {
  name = "rappi_driver_db"

  parameters = {
    "locationUri" = "s3://rodaapp-rappidriverchain/athena-results/"
  }
}

resource "aws_glue_catalog_table" "rappi_driver_routes" {
  name          = "routes"
  database_name = aws_glue_catalog_database.rappi_driver_db.name

  table_type = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://rodaapp-rappidriverchain/rappi_driver_routes/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
        name                  = "CSV"
        serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        parameters            = {
            "field.delim" = ","
            "serialization.format" = ","
            "skip.header.line.count"  = "1"  # Ignorar la primera fila (encabezado)
        }
    }

    columns {
      name = "routeid"
      type = "int"
    }

    columns {
      name = "gpsid"
      type = "string"
    }

    columns {
      name = "timestampstart"
      type = "bigint"
    }

    columns {
      name = "timestampend"
      type = "bigint"
    }

    columns {
      name = "measureddistance"
      type = "int"
    }

    columns {
      name = "celo_address"
      type = "string"
    }
  }

  partition_keys {
    name = "date"
    type = "string"
  }

  partition_keys {
    name = "source"
    type = "string"
  }
}