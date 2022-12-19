use std::io::Error as IoError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    /// There was a error processing io
    #[error("Couldn't process the specified stream: {0}")]
    Io(#[from] IoError),
    // /// There was a error making or processing http
    // #[error("There was an error with the remote request: {0}")]
    // Http(#[from] HttpError),
}

impl From<Error> for String {
    fn from(e: Error) -> String {
        e.to_string()
    }
}
