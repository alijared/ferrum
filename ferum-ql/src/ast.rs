use std::collections::HashSet;

pub struct Query {
    pub selector: Selector,
    pub map_functions: HashSet<Function>,
}

#[derive(Default, Clone)]
pub struct Selector {
    pub level: Option<Filter>,
    pub message: Option<Filter>,
    pub attributes: Vec<Filter>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub key: String,
    pub op: ComparisonOp,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOp {
    Eq,
    Neq,
    Regex,
    Greater,
    GreaterEq,
    Less,
    LessEq,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Function {
    Count,
    Json,
}
