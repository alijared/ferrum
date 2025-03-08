mod ast;

use crate::ast::Query;
use crate::grammar::QueryParser;
pub use ast::{ComparisonOp, Filter, Function};
use lalrpop_util::lexer::Token;
use lalrpop_util::{lalrpop_mod, ParseError};

lalrpop_mod!(
    #[allow(clippy::ptr_arg, unused_mut)]
    #[rustfmt::skip]
    grammar
);

pub fn parse(query: &str) -> Result<Query, ParseError<usize, Token<'_>, &'static str>> {
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
        assert_eq!(
            selector.level,
            Some(Filter {
                key: "level".to_string(),
                op: ComparisonOp::Eq,
                value: "DEBUG".to_string()
            })
        );
        assert_eq!(selector.message, None);
        assert_eq!(selector.attributes.len(), 0);

        let query = match QueryParser::new().parse(r#"{message="Jaaaa", fish!="biscuit"}"#) {
            Ok(q) => q,
            Err(e) => panic!("{:?}", e),
        };

        let selector = query.selector;
        assert_eq!(selector.level, None,);
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

        let query = match QueryParser::new().parse(r#"{message="Jaaaa", fish!="biscuit"} | json"#) {
            Ok(q) => q,
            Err(e) => panic!("{:?}", e),
        };
        
        let selector = query.selector;
        assert_eq!(selector.level, None,);
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
            query
                .map_functions
                .get(&ast::Function::Json)
                .cloned()
                .unwrap(),
            ast::Function::Json
        );
    }
}
