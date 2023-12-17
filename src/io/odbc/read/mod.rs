//! APIs to read from ODBC
mod deserialize;
mod schema;

pub use deserialize::deserialize;
pub use schema::infer_schema;

pub use super::api::buffers::{BufferDesc, ColumnarAnyBuffer};
pub use super::api::ColumnDescription;
pub use super::api::Error;
pub use super::api::ResultSetMetadata;

use crate::array::Array;
use crate::chunk::Chunk;
use crate::error::Result;
use crate::io::odbc::api::{Connection, ConnectionOptions, Cursor, Environment};

struct Reader {
    connection_string: String,
    query: String,
    connection_options: ConnectionOptions,
}

impl Reader {
    pub fn new(connection_string: String, query: String, login_timeout_sec: Option<u32>) -> Self {
        Self {
            connection_string: connection_string,
            query: query,
            connection_options: ConnectionOptions {
                login_timeout_sec: login_timeout_sec,
            },
        }
    }

    pub fn read(&self, max_batch_size: Option<usize>) -> Result<Vec<Chunk<Box<dyn Array>>>> {
        let env = Environment::new().unwrap();
        let conn: Connection = env
            .connect_with_connection_string(
                self.connection_string.as_str(),
                self.connection_options,
            )
            .unwrap();

        // conn.execute(query, ()).unwrap();

        let mut a = conn.prepare(self.query.as_str()).unwrap();
        let fields = infer_schema(&mut a)?;

        let buffer = buffer_from_metadata(&mut a, max_batch_size.unwrap_or(100)).unwrap();

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
/// Iff the driver provides an incorrect [`api::ResultSetMetadata`]
pub fn buffer_from_metadata(
    result_set_metadata: &mut impl ResultSetMetadata,
    capacity: usize,
) -> std::result::Result<ColumnarAnyBuffer, Error> {
    let num_cols: u16 = result_set_metadata.num_result_cols()? as u16;

    let col_descs = vec![ColumnDescription::default(); num_cols as usize];

    for (i, col_desc) in col_descs.iter().enumerate() {
        result_set_metadata.describe_col((i + 1) as u16, &mut col_desc.clone())?;
    }

    let descs = col_descs.into_iter().map(|description| {
        BufferDesc::from_data_type(description.data_type, description.could_be_nullable()).unwrap()
    });

    Ok(ColumnarAnyBuffer::from_descs(capacity, descs))
}
