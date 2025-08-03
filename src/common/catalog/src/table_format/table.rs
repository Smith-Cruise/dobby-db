
#[derive(Debug)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String
}

impl TableIdentifier {
    pub fn new(db_name: &str, tbl_name: &str) -> Self {
        TableIdentifier {
            namespace: vec![db_name.to_string()],
            name: tbl_name.to_string()
        }
    }
}