use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub type Header = u32;

pub const HEADER_SIZE: usize = std::mem::size_of::<Header>();

pub struct TypeToBytes<T>
where
    T: Serialize
{
    _phantom: PhantomData<T>,
}

pub struct BytesToType<T>
where
    T: for<'de> Deserialize<'de>
{
    _phantom: PhantomData<T>,
}

impl<T> Default for TypeToBytes<T>
where
    T: Serialize
{
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Default for BytesToType<T>
where
    T: for<'de> Deserialize<'de>
{
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Encoder<T> for TypeToBytes<T>
where
    T: Serialize
{
    type Error = bincode::Error;

    fn encode(
        &mut self,
        item: T,
        dst: &mut BytesMut
    ) -> Result<(), Self::Error> {
        // convert to bytes first
        let buf = bincode::serialize(&item)?;

        // save the length of the buffer as a slice
        let len_slice = Header::to_be_bytes(buf.len() as Header);

        // reserve enough memory in dst for header and actual buf so that
        // our two resize operations below don't actually cause two reallocs
        dst.reserve(len_slice.len() + buf.len());
        
        // build the actual payload
        dst.extend_from_slice(&len_slice);
        dst.extend_from_slice(&buf);

        Ok(())
    }
}

impl<T> Decoder for BytesToType<T>
where
    T: for<'de> Deserialize<'de>
{
    type Item = T;
    type Error = bincode::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut
    ) -> Result<Option<T>, Self::Error> {
        // ensure we can read the header
        if src.len() < std::mem::size_of::<Header>() {
            return Ok(None);
        }

        // read header containing length, without consuming it
        let len_slice = &src[..std::mem::size_of::<Header>()];
        let len = Header::from_be_bytes(len_slice.try_into().unwrap());

        // ensure we have enough bytes to read the payload
        if src.len() < HEADER_SIZE + len as usize {
            // full payload has not arrived yet. we also reserve space for
            // the remainder of the payload in the buffer, although this is not
            // strictly necessary
            src.reserve(len as usize);

            return Ok(None);
        }

        // attempt to deserialize the payload
        let buf = &src[HEADER_SIZE..(HEADER_SIZE + len as usize)];
        let item: T = bincode::deserialize(buf)?;

        // consume the bytes we just read
        src.advance(HEADER_SIZE + len as usize);

        Ok(Some(item))
    }
}
