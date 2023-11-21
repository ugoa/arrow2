//! APIs to read from ODBC
mod deserialize;
mod schema;

pub use deserialize::deserialize;
pub use schema::infer_schema;

pub use super::api::buffers::{BufferDesc, ColumnarAnyBuffer};
pub use super::api::ColumnDescription;
pub use super::api::Error;
pub use super::api::ResultSetMetadata;

use crate::array::{Array, BinaryArray, BooleanArray, Int32Array, Int64Array, Utf8Array};
use crate::chunk::Chunk;
use crate::datatypes::{DataType, Field, TimeUnit};
use crate::error::Result;
use crate::io::odbc::api::{Connection, ConnectionOptions, Cursor, Environment};
// use crate::io::odbc::read::{buffer_from_metadata, deserialize, infer_schema};

struct Reader;

impl Reader {
    pub fn read(
        connection_string: &str,
        login_timeout_sec: Option<u32>,
        query: &str,
    ) -> Result<Vec<Chunk<Box<dyn Array>>>> {
        let env: Environment = Environment::new().unwrap();
        let conn = env
            .connect_with_connection_string(connection_string, ConnectionOptions::default())
            .unwrap();

        conn.execute(query, ()).unwrap();

        let mut a = conn.prepare(query).unwrap();
        let fields = infer_schema(&a)?;

        let max_batch_size = 100;
        let buffer = buffer_from_metadata(&a, max_batch_size).unwrap();

        let cursor = a.execute(()).unwrap().unwrap();
        let mut cursor = cursor.bind_buffer(buffer).unwrap();

        let mut chunks = vec![];
        while let Some(batch) = cursor.fetch().unwrap() {
            let arrays = (0..batch.num_cols())
                .zip(fields.iter())
                .map(|(index, field)| {
                    let column_view = batch.column(index);
                    deserialize(column_view, field.data_type.clone())
                })
                .collect::<Vec<_>>();
            chunks.push(Chunk::new(arrays));
        }

        Ok(chunks)
    }
}

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
