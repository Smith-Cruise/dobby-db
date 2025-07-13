use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum DobbyDBError {
    InvalidArgument(String)
}

impl Display for DobbyDBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DobbyDBError::InvalidArgument(err) => {
                write!(f, "invalid argument: {}", err)
            }
        }
    }
}

impl std::error::Error for DobbyDBError {

}