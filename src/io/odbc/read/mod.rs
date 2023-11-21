//! APIs to read from ODBC
mod deserialize;
mod schema;

pub use deserialize::deserialize;
pub use schema::infer_schema;

pub use super::api::buffers::{BufferDesc, ColumnarAnyBuffer};
pub use super::api::ColumnDescription;
pub use super::api::Error;
pub use super::api::ResultSetMetadata;

/// Creates a [`api::buffers::ColumnarBuffer`] from the metadata.
/// # Errors
/// If the driver provides an incorrect [`api::ResultSetMetadata`]
pub fn buffer_from_metadata(
    resut_set_metadata: &impl ResultSetMetadata,
    capacity: usize,
) -> std::result::Result<ColumnarAnyBuffer, Error> {
    let num_cols: u16 = resut_set_metadata.num_result_cols()? as u16;

    let col_descs = vec![ColumnDescription::default(); num_cols as usize];

    for (i, mut col_desc) in col_descs.iter().enumerate() {
        resut_set_metadata.describe_col((i + 1) as u16, &mut col_desc)?;
    }

    let descs = col_descs.into_iter().map(|description| {
        BufferDesc::from_data_type(description.data_type, description.could_be_nullable()).unwrap()
    });

    Ok(ColumnarAnyBuffer::from_descs(capacity, descs))
}
