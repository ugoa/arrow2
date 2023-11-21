//! APIs to write to ODBC
mod schema;
mod serialize;

use crate::{array::Array, chunk::Chunk, datatypes::Field, error::Result};

use super::api;
use crate::io::odbc::api::{Connection, ConnectionOptions, Environment};
pub use api::buffers::{BufferDesc, ColumnarAnyBuffer};
pub use api::ColumnDescription;
pub use schema::infer_descriptions;
pub use serialize::serialize;

/// Creates a [`api::buffers::ColumnarBuffer`] from [`api::ColumnDescription`]s.
///
/// This is useful when separating the serialization (CPU-bounded) to writing to the DB (IO-bounded).
pub fn buffer_from_description(
    descriptions: Vec<ColumnDescription>,
    capacity: usize,
) -> ColumnarAnyBuffer {
    let descs = descriptions.into_iter().map(|description| {
        BufferDesc::from_data_type(description.data_type, description.could_be_nullable()).unwrap()
    });

    ColumnarAnyBuffer::from_descs(capacity, descs)
}

/// A writer of [`Chunk`]s to an ODBC [`api::Prepared`] statement.
/// # Implementation
/// This struct mixes CPU-bounded and IO-bounded tasks and is not ideal
/// for an `async` context.
pub struct Writer {
    // fields: Vec<Field>,
    // prepared: api::Prepared<S>,
    connection_string: String,
    env: Environment,
    connection_options: ConnectionOptions,
}

impl Writer {
    pub fn new(connection_string: String, login_timeout_sec: Option<u32>) -> Self {
        Self {
            connection_string: connection_string,
            env: Environment::new().unwrap(),
            connection_options: ConnectionOptions {
                login_timeout_sec: login_timeout_sec,
            },
        }
    }

    /// Writes a chunk to the writer.
    /// # Errors
    /// Errors iff the execution of the statement fails.
    pub fn write<A: AsRef<dyn Array>>(
        &mut self,
        fields: Vec<Field>,
        chunk: &Chunk<A>,
        query: &str,
    ) -> Result<()> {
        let conn: Connection = self
            .env
            .connect_with_connection_string(
                self.connection_string.as_str(),
                self.connection_options,
            )
            .unwrap();

        let buf_descs = infer_descriptions(&fields)?.into_iter().map(|description| {
            BufferDesc::from_data_type(description.data_type, description.could_be_nullable())
                .unwrap()
        });

        let prepared = conn.prepare(query).unwrap();
        let mut prebound = prepared
            .into_column_inserter(chunk.len(), buf_descs)
            .unwrap();
        prebound.set_num_rows(chunk.len());

        for (i, column) in chunk.arrays().iter().enumerate() {
            serialize(column.as_ref(), &mut prebound.column_mut(i)).unwrap();
        }
        prebound.execute().unwrap();

        Ok(())
    }
}
