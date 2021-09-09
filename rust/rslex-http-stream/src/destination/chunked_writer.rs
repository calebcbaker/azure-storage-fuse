use rslex_core::file_io::OutputStream;
use std::{io::Write, sync::Arc};

pub struct ChunkedWriter<T: Write + Send> {
    resource_id: Arc<str>,
    bufwriter: std::io::BufWriter<T>,
    chunk_size: usize,
}

impl<T: Write + Send> Write for ChunkedWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let critical = self.chunk_size - self.bufwriter.buffer().len();

        Ok(if buf.len() < critical {
            self.bufwriter.write(buf)?
        } else {
            let (left, right) = buf.split_at(critical);
            self.bufwriter.write(left)?
                + right
                    .chunks(self.chunk_size)
                    .try_fold(0, |bytes, chunk| self.bufwriter.write(chunk).map(|r| r + bytes))?
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.bufwriter.flush()
    }
}

impl<T: Write + Send> OutputStream for ChunkedWriter<T> {
    fn resource_id(&self) -> &str {
        self.resource_id.as_ref()
    }
}

impl<T: Write + Send> ChunkedWriter<T> {
    pub fn new(resource_id: Arc<str>, underlying_writer: T, chunk_size: usize) -> Self {
        debug_assert!(
            chunk_size > 0,
            "ChunkedWriter should be initialized with non-zero positive capacity."
        );

        ChunkedWriter::<T> {
            resource_id,
            bufwriter: std::io::BufWriter::with_capacity(chunk_size, underlying_writer),
            chunk_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkedWriter;
    use fluent_assertions::*;
    use std::io::Write;

    #[derive(Debug, Copy, Clone)]
    struct FakeADLSGen1Writer {
        pub max_byte_written: usize,
        pub total_byte_written: usize,
    }

    impl Write for FakeADLSGen1Writer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.len() > self.max_byte_written {
                self.max_byte_written = buf.len();
            }

            self.total_byte_written = self.total_byte_written + buf.len();
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    macro_rules! chunk_writer_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let (write_size, chunk_size) = $value;

                let mut chunked_writer = ChunkedWriter::new(
                    String::new().into(),
                    FakeADLSGen1Writer {
                        max_byte_written: 0,
                        total_byte_written: 0,
                    },
                    chunk_size,
                );

                chunked_writer
                    .write(&vec![0; write_size][..])
                    .expect("Chunked writer writes should succedd.");
                chunked_writer
                    .flush()
                    .expect("Chunked writer flush should succedd");

                chunked_writer.bufwriter
                    .into_inner()
                    .expect("Getting underlying writer from bufwriter should succedd.")
                    .should()
                    .pass(|w| {
                        w.total_byte_written.should().be(write_size);
                        w.max_byte_written.should().be(std::cmp::min(chunk_size, write_size));
                });
            }
        )*
        }
    }

    chunk_writer_tests! {
        chunked_writer_writes_less_than_chunk_size: (3, 4),
        chunked_writer_writes_exactly_chunk_size: (4, 4),
        chunked_writer_writes_less_than_two_chunk_size: (7, 4),
        chunked_writer_writes_exactly_two_chunk_size: (8, 4),
    }

    #[test]
    #[cfg(debug_assertions)]
    fn chunked_writer_should_panic_if_initialized_with_zero_capacity() {
        (|| {
            ChunkedWriter::new(
                String::new().into(),
                FakeADLSGen1Writer {
                    max_byte_written: 0,
                    total_byte_written: 0,
                },
                0,
            );
        })
        .should()
        .panic()
        .with_message("ChunkedWriter should be initialized with non-zero positive capacity.");
    }
}
