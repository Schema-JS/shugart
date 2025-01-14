use enum_as_inner::EnumAsInner;
use crate::cursor::Cursor;
use crate::U64_SIZE;

pub struct DiskMetadataV1 {
    pub created_at: u64
}

#[derive(EnumAsInner)]
pub enum DiskMetadata {
    V1(DiskMetadataV1)
}

impl DiskMetadata {

    pub fn get_le_identifier(&self) -> [u8; 1] {
        match &self {
            DiskMetadata::V1(_) => [0u8]
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut vec = vec![];

        match &self {
            DiskMetadata::V1(data) => {
                vec.extend_from_slice(&self.get_le_identifier());
                vec.extend_from_slice(&data.created_at.to_le_bytes());
            }
        }

        vec
    }

    pub fn size(&self) -> usize {
        match &self {
            DiskMetadata::V1(_) => {
                // created_at
                U64_SIZE
            }
        }
    }

}

impl TryFrom<Vec<u8>> for DiskMetadata {
    type Error = ();

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let mut cursor = Cursor::new(&value);
        let le_identifier = cursor.consume(1).unwrap();
        match le_identifier.get(0).unwrap() {
            0u8 => {
                let created_at_le_bytes = cursor.consume(U64_SIZE).unwrap();
                let created_at = u64::from_le_bytes(created_at_le_bytes.try_into().unwrap());
                Ok(DiskMetadata::V1(DiskMetadataV1 {
                    created_at,
                }))
            }
            _ => Err(())
        }
    }
}