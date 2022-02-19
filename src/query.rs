use nom::{
    branch::alt,
    bytes::complete::is_not,
    character::complete::char,
    combinator::all_consuming,
    multi::separated_list0,
    sequence::{preceded, terminated},
    IResult,
};

fn quoted_key(input: &str) -> IResult<&str, &str> {
    preceded(char('\"'), terminated(is_not("\""), char('\"')))(input)
}

fn key(input: &str) -> IResult<&str, &str> {
    is_not("\"\n\t .")(input)
}

pub fn query(input: &str) -> IResult<&str, Vec<&str>> {
    all_consuming(preceded(
        char('.'),
        separated_list0(char('.'), alt((quoted_key, key))),
    ))(input)
}

#[test]
fn parse_query() {
    assert_eq!(query("a.b.v"), Ok(("", vec!["a", "b", "v"])));
}
#[test]
fn parse_quoted_query() {
    assert_eq!(query("a.\"b\".v"), Ok(("", vec!["a", "b", "v"])));
}
#[test]
fn parse_quoted_dot_query() {
    assert_eq!(query("a.\"b.b\".v"), Ok(("", vec!["a", "b.b", "v"])));
}
#[test]
fn parse_quoted_dot_query2() {
    assert_eq!(
        query("a.\"b\n.\t  b\".v"),
        Ok(("", vec!["a", "b\n.\t  b", "v"]))
    );
}
