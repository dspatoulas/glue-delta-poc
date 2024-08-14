use aws_sdk_glue::{Client as GlueClient, Config as GlueConfig};
use aws_config::BehaviorVersion;
use aws_sdk_glue::types::{Column, SerDeInfo, StorageDescriptor, TableInput};
use tracing::info;

#[tokio::main]
async fn main() {
    const TABLE_NAME: &str = "events";
    const TABLE_PATH: &str = "s3://ally-security-development-events/delta/events";

    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    let glue_config = GlueConfig::new(&aws_config);

    let glue_client = GlueClient::from_conf(glue_config);

    let columns = vec![
        create_column("id", "string", false),
        create_column("time", "timestamp", false),
        create_column("trace_id", "string", true),
        create_column("source", "string", false),
        create_column("detail_type", "string", false),
    ];

    let serde_info = SerDeInfo::builder()
        .serialization_library("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
        .build();

    let storage_descriptor = StorageDescriptor::builder()
        .location(TABLE_PATH)
        .set_columns(Some(columns))
        .input_format("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
        .output_format("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
        .serde_info(serde_info)
        // Additional properties like location, input/output formats, etc., can be set here
        .build();

    let table_input = TableInput::builder()
        .name(TABLE_NAME)
        .storage_descriptor(storage_descriptor)
        .parameters("classification", "delta")
        // According to AWS, this should be set for lake formation support...
        // but it doesn't work and makes queries in Athena fail with an error
        // https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables-syncing-metadata.html
        // .parameters("spark.sql.sources.provider", "delta")

        // It specifically says __not__ to use this one, but it works
        // and the other `table_type` doesn't... so we're using it 😈
        .parameters("table_type", "DELTA")
        .build()
        .expect("must build a valid table input");

    glue_client
        .create_table()
        .database_name("ally_security")
        .table_input(table_input)
        .send()
        .await
        .unwrap();

    info!(table_name = TABLE_NAME, "Created table in AWS Glue");
}

fn create_column(name: &str, data_type: &str, nullable: bool) -> Column {
    Column::builder()
        .name(name)
        .r#type(data_type)
        .parameters("nullable", nullable.to_string())
        .build()
        .unwrap()
}




