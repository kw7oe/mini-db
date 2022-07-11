use crate::BigArray;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
pub const ROW_SIZE: usize = USERNAME_SIZE + EMAIL_SIZE + 4 + std::mem::size_of::<bool>(); // u32 is 4 x u8;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Row {
    pub id: u32,
    #[serde(with = "BigArray")]
    pub username: [u8; USERNAME_SIZE],
    #[serde(with = "BigArray")]
    pub email: [u8; EMAIL_SIZE],
    pub is_deleted: bool,
}

impl Row {
    pub fn new(id: &str, u: &str, m: &str) -> Result<Row, String> {
        let id = id
            .parse::<u32>()
            .map_err(|_e| "invalid id provided".to_string())?;

        let mut username: [u8; USERNAME_SIZE] = [0; USERNAME_SIZE];
        let mut email: [u8; EMAIL_SIZE] = [0; EMAIL_SIZE];

        let mut index = 0;
        for c in u.bytes() {
            username[index] = c;
            index += 1;
        }

        index = 0;
        for c in m.bytes() {
            email[index] = c;
            index += 1;
        }

        Ok(Row {
            is_deleted: false,
            id,
            username,
            email,
        })
    }

    pub fn update(&mut self, column: &str, new_row: &Row) {
        match column {
            "username" => {
                self.username = new_row.username;
            }
            "email" => {
                self.email = new_row.email;
            }
            _ => panic!("invalid column name: {}", column),
        }
    }

    pub fn username(&self) -> String {
        // Since we are converting from a fixed size array, there will be NULL
        // characters at the end. Hence, we need to trim it.
        //
        // While it doesn't impact outputing to the display, it caused
        // issue with our test, as the result will have additional character while
        // our expectation don't.
        String::from_utf8_lossy(&self.username)
            .trim_end_matches(char::from(0))
            .to_owned()
    }

    pub fn email(&self) -> String {
        String::from_utf8_lossy(&self.email)
            .trim_end_matches(char::from(0))
            .to_owned()
    }
}

impl FromStr for Row {
    type Err = String;

    fn from_str(row: &str) -> Result<Self, Self::Err> {
        let columns: Vec<&str> = row.split(' ').collect();
        match columns[..] {
            [id] => Self::new(id, "", ""),
            [id, name, email] => {
                if name.len() > USERNAME_SIZE {
                    return Err("Name is too long.".to_string());
                }

                if email.len() > EMAIL_SIZE {
                    return Err("Email is too long.".to_string());
                }

                Self::new(id, name, email)
            }
            _ => Err(format!("Unrecognized keyword at start of '{row}'.")),
        }
    }
}

impl std::string::ToString for Row {
    fn to_string(&self) -> String {
        format!("({}, {}, {})", self.id, self.username(), self.email())
    }
}
