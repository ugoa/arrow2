//! APIs to write to ODBC
mod schema;
mod serialize;

use crate::{array::Array, chunk::Chunk, datatypes::Field, error::Result};

use super::api;
pub use api::buffers::{BufferDesc, ColumnarAnyBuffer};
pub use api::ColumnDescription;
use api::RowSetBuffer;
pub use schema::infer_descriptions;
pub use serialize::{serialize, serialize2};

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
pub struct Writer<S> {
    fields: Vec<Field>,
    buffer: ColumnarAnyBuffer,
    prepared: api::Prepared<S>,
}

impl<S> Writer<S>
where
    S: api::handles::AsStatementRef,
{
    /// Creates a new [`Writer`].
    /// # Errors
    /// Errors iff any of the types from [`Field`] is not supported.
    pub fn try_new(prepared: api::Prepared<S>, fields: Vec<Field>) -> Result<Self> {
        let buffer = buffer_from_description(infer_descriptions(&fields)?, 0);

        Ok(Self {
            fields,
            buffer,
            prepared,
        })
    }

    /// Writes a chunk to the writer.
    /// # Errors
    /// Errors iff the execution of the statement fails.
    pub fn write<A: AsRef<dyn Array>>(&mut self, chunk: &Chunk<A>) -> Result<()> {
        let buf_descs = infer_descriptions(&self.fields)?
            .into_iter()
            .map(|description| {
                BufferDesc::from_data_type(description.data_type, description.could_be_nullable())
                    .unwrap()
            });

        let mut prebound = self
            .prepared
            .into_column_inserter(chunk.len(), buf_descs)
            .unwrap();

        prebound.set_num_rows(chunk.len());

        for (i, column) in chunk.arrays().iter().enumerate() {
            serialize2(column.as_ref(), &mut prebound.column_mut(i));
        }
        prebound.execute().unwrap();

        // serialize (CPU-bounded)
        // for (i, column) in chunk.arrays().iter().enumerate() {
        //     serialize(column.as_ref(), &mut self.buffer.column(i).as_view_mut)?;
        // }
        Ok(())
    }
}
