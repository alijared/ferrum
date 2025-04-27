mod ast;
mod error;

use crate::grammar::QueryParser;
pub use ast::{ComparisonOp, Filter, Function, Query};
pub use error::Error;
use lalrpop_util::lalrpop_mod;

lalrpop_mod!(
    #[allow(clippy::ptr_arg, unused_mut)]
    #[rustfmt::skip]
    grammar
);

pub fn parse(query: &str) -> Result<Query, Error> {
    QueryParser::new().parse(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ComparisonOp, Filter};

    #[test]
    fn test_parse_lalrpop() {
        let query = match QueryParser::new().parse(r#"{level="DEBUG"}"#) {
            Ok(q) => q,
            Err(e) => panic!("{:?}", e),
        };

        let selector = query.selector;
        assert_eq!(selector.message, None);
        assert_eq!(selector.attributes.len(), 1);

        let query = match QueryParser::new().parse(r#"{message="Jaaaa", fish!="biscuit"}"#) {
            Ok(q) => q,
            Err(e) => panic!("{:?}", e),
        };

        let selector = query.selector;
        assert_eq!(
            selector.message,
            Some(Filter {
                key: "message".to_string(),
                op: ComparisonOp::Eq,
                value: "Jaaaa".to_string()
            })
        );
        assert_eq!(selector.attributes.len(), 1);
        assert_eq!(
            *selector.attributes.first().unwrap(),
            Filter {
                key: "fish".to_string(),
                op: ComparisonOp::Neq,
                value: "biscuit".to_string(),
            }
        );

        let query = match QueryParser::new().parse(r#"{message="Jaaaa", fish!="biscuit"} | count"#)
        {
            Ok(q) => q,
            Err(e) => panic!("{:?}", e),
        };

        let selector = query.selector;
        assert_eq!(
            selector.message,
            Some(Filter {
                key: "message".to_string(),
                op: ComparisonOp::Eq,
                value: "Jaaaa".to_string()
            })
        );
        assert_eq!(selector.attributes.len(), 1);
        assert_eq!(
            *selector.attributes.first().unwrap(),
            Filter {
                key: "fish".to_string(),
                op: ComparisonOp::Neq,
                value: "biscuit".to_string(),
            }
        );

        assert_eq!(query.map_functions.len(), 1);
        assert_eq!(
            query.map_functions.get(&Function::Count).cloned().unwrap(),
            Function::Count
        );
    }
}
