use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until, take_while1};
use nom::character::complete::{char, multispace0};
use nom::combinator::{map, opt, value};
use nom::multi::{separated_list0, separated_list1};
use nom::sequence::{delimited, preceded, tuple};
use nom::IResult;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub struct Query {
    pub level: Option<Filter<String>>,
    pub message: Option<Filter<String>>,
    pub attributes: Vec<(String, Filter<String>)>,
    pub functions: HashMap<Function, i8>,
}

#[derive(Debug, PartialEq)]
pub struct Filter<T> {
    pub op: Operation,
    pub value: T,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Eq,
    Neq,
    Regex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Function {
    Count,
    Json,
}

pub fn parse(query: &str) -> Result<Query, nom::Err<nom::error::Error<&str>>> {
    let (remaining, filters) = parse_query(query)?;
    let (_, functions) = parse_functions(remaining)?;

    Ok(build_query(filters, functions))
}

fn parse_query(input: &str) -> IResult<&str, Vec<(&str, Filter<String>)>> {
    delimited(
        char('{'),
        separated_list0(delimited(multispace0, char(','), multispace0), parse_filter),
        char('}'),
    )(input)
}

fn parse_functions(input: &str) -> IResult<&str, Vec<Function>> {
    let (remaining, functions) = opt(preceded(
        whitespace(char('|')),
        separated_list1(whitespace(char('|')), whitespace(parse_function)),
    ))(input)?;

    Ok((remaining, functions.unwrap_or_default()))
}

fn build_query(filters: Vec<(&str, Filter<String>)>, functions: Vec<Function>) -> Query {
    let mut query = Query {
        level: None,
        message: None,
        attributes: Vec::new(),
        functions: HashMap::new(),
    };

    for (key, filter) in filters {
        match key {
            "level" => query.level = Some(filter),
            "message" => query.message = Some(filter),
            _ => query.attributes.push((key.to_string(), filter)),
        }
    }

    for f in functions {
        query.functions.insert(f, 0);
    }

    query
}

fn parse_filter(input: &str) -> IResult<&str, (&str, Filter<String>)> {
    let (remaining, (key_name, op, value_str)) = tuple((
        parse_key,
        delimited(multispace0, parse_operator, multispace0),
        parse_string,
    ))(input)?;

    Ok((
        remaining,
        (
            key_name,
            Filter {
                op,
                value: value_str,
            },
        ),
    ))
}

fn parse_key(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.')(input)
}

fn parse_operator(input: &str) -> IResult<&str, Operation> {
    alt((
        value(Operation::Eq, tag("=")),
        value(Operation::Neq, tag("!=")),
        value(Operation::Regex, tag("~=")),
    ))(input)
}

fn parse_string(input: &str) -> IResult<&str, String> {
    map(
        delimited(char('"'), take_until("\""), char('"')),
        |s: &str| s.to_string(),
    )(input)
}

fn parse_function(input: &str) -> IResult<&str, Function> {
    alt((
        value(Function::Count, tag_no_case("count")),
        value(Function::Json, tag_no_case("json")),
    ))(input)
}

fn whitespace<'a, F, O>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O>
where
    F: FnMut(&'a str) -> IResult<&'a str, O>,
{
    delimited(multispace0, inner, multispace0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let result = parse(r#"{level="DEBUG", fish="biscuit", turnip!="juice"} | count | json"#);
        assert!(result.is_ok());

        let query = result.unwrap();
        let mut functions = HashMap::with_capacity(2);
        functions.insert(Function::Count, 0);
        functions.insert(Function::Json, 0);
        
        assert_eq!(
            query,
            Query {
                level: Some(Filter {
                    op: Operation::Eq,
                    value: "DEBUG".to_string()
                }),
                message: None,
                attributes: vec![
                    (
                        "fish".into(),
                        Filter {
                            op: Operation::Eq,
                            value: "biscuit".to_string()
                        }
                    ),
                    (
                        "turnip".into(),
                        Filter {
                            op: Operation::Neq,
                            value: "juice".to_string()
                        }
                    )
                ],
                functions: functions,
            }
        );

        let result = parse(r#"{level="DEBUG", message!="hello world"}"#);
        assert!(result.is_ok());

        let query = result.unwrap();
        assert_eq!(
            query,
            Query {
                level: Some(Filter {
                    op: Operation::Eq,
                    value: "DEBUG".to_string()
                }),
                message: Some(Filter {
                    op: Operation::Neq,
                    value: "hello world".to_string()
                }),
                attributes: Vec::new(),
                functions: HashMap::new(),
            }
        );

        let result = parse(r#"{level = "DEBUG",message!="hello world"}"#);
        assert!(result.is_ok());

        let query = result.unwrap();
        assert_eq!(
            query,
            Query {
                level: Some(Filter {
                    op: Operation::Eq,
                    value: "DEBUG".to_string()
                }),
                message: Some(Filter {
                    op: Operation::Neq,
                    value: "hello world".to_string()
                }),
                attributes: Vec::new(),
                functions: HashMap::new(),
            }
        );
    }

    #[test]
    fn test_parse_filter() {
        let (_, (key_name, filter)) = parse_filter("level=\"error\"").unwrap();
        assert_eq!(key_name, "level");
        assert!(matches!(filter.op, Operation::Eq));
        assert_eq!(filter.value, "error");

        let (_, (key_name, filter)) = parse_filter("message!=\"success\"").unwrap();
        assert_eq!(key_name, "message");
        assert!(matches!(filter.op, Operation::Neq));
        assert_eq!(filter.value, "success");

        let (_, (key_name, filter)) = parse_filter("message~=\"connection.*\"").unwrap();
        assert_eq!(key_name, "message");
        assert!(matches!(filter.op, Operation::Regex));
        assert_eq!(filter.value, "connection.*");

        let (_, (key_name, filter)) = parse_filter("level = \"error\"").unwrap();
        assert_eq!(key_name, "level");
        assert_eq!(filter.op, Operation::Eq);
        assert_eq!(filter.value, "error");

        let (_, (key_name, filter)) = parse_filter("message  !=  \"success\"").unwrap();
        assert_eq!(key_name, "message");
        assert_eq!(filter.op, Operation::Neq);
        assert_eq!(filter.value, "success");
    }

    #[test]
    fn test_parse_key() {
        assert_eq!(parse_key("level"), Ok(("", "level")));
        assert_eq!(parse_key("message123"), Ok(("", "message123")));
        assert_eq!(parse_key("first_name"), Ok(("", "first_name")));
    }

    #[test]
    fn test_parse_operator() {
        assert_eq!(parse_operator("="), Ok(("", Operation::Eq)));
        assert_eq!(parse_operator("!="), Ok(("", Operation::Neq)));
        assert_eq!(parse_operator("~="), Ok(("", Operation::Regex)));
    }

    #[test]
    fn test_parse_string() {
        assert_eq!(parse_string("\"error\""), Ok(("", "error".to_string())));
        assert_eq!(
            parse_string("\"complex value with spaces\""),
            Ok(("", "complex value with spaces".to_string()))
        );
        assert_eq!(parse_string("\"\""), Ok(("", "".to_string())));
        assert!(parse_string("hello world").is_err());
        assert!(parse_string("\"hello world").is_err());
        assert!(parse_string("hello world\"").is_err());
    }

    #[test]
    fn test_parse_functions() {
        assert_eq!(parse_functions("| count"), Ok(("", vec![Function::Count])));
        assert_eq!(
            parse_functions("| count | json"),
            Ok(("", vec![Function::Count, Function::Json]))
        );
        assert_eq!(
            parse_functions(" |  count  | json "),
            Ok(("", vec![Function::Count, Function::Json]))
        );
    }

    #[test]
    fn test_parse_function() {
        assert_eq!(parse_function("count"), Ok(("", Function::Count)));
        assert_eq!(parse_function("Json"), Ok(("", Function::Json)));
        assert!(parse_function("non-existent function").is_err());
    }
}
