use lalrpop_util::lexer::Token;
use lalrpop_util::ParseError;

pub type Error<'a> = ParseError<usize, Token<'a>, &'static str>;
