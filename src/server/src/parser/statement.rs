 use sqlparser::ast::Statement as SQLStatement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// ANSI SQL AST node (from sqlparser-rs)
    Statement(Box<SQLStatement>),

    ShowCatalogsStatement(ShowCatalogsStatement)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowCatalogsStatement {

}