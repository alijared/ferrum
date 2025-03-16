use crate::io;
use crate::io::query::{FromStreams, Log};
use crate::io::{query, tables};
use chrono::{DateTime, Utc};
use chrono::{Datelike, TimeZone};
use datafusion::common::{JoinType, ScalarValue};
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::functions_aggregate::array_agg::array_agg;
use datafusion::functions_nested;
use datafusion::logical_expr::{col, lit, lit_timestamp_nano, Expr, Literal};
use datafusion::prelude::{regexp_match, SessionContext};
use serde::de::Visitor;
use serde::{de, Deserialize, Deserializer};
use std::fmt;

#[derive(Deserialize)]
pub struct QueryParams {
    pub query: Option<String>,
    #[serde(flatten)]
    pub time_range: TimeRangeQueryParams,
    pub sort: Option<SortOrder>,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct QueryAttributeValuesParams {
    #[serde(flatten)]
    pub time_range: TimeRangeQueryParams,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct TimeRangeQueryParams {
    #[serde(alias = "start", deserialize_with = "deserialize_datetime")]
    pub from: DateTime<Utc>,
    #[serde(alias = "end", deserialize_with = "deserialize_datetime")]
    pub to: DateTime<Utc>,
}

fn deserialize_datetime<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    struct DateTimeVisitor;

    impl<'de> Visitor<'de> for DateTimeVisitor {
        type Value = DateTime<Utc>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an RFC3339 string or Unix timestamp in nanoseconds")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if let Ok(ts) = value.parse::<i64>() {
                return Ok(Utc.timestamp_nanos(ts));
            }
            value.parse::<DateTime<Utc>>().map_err(de::Error::custom)
        }
    }

    deserializer.deserialize_any(DateTimeVisitor)
}

#[derive(Clone, Deserialize)]
pub enum SortOrder {
    #[serde(rename = "asc", alias = "forward")]
    Ascending,
    #[serde(rename = "desc", alias = "backward")]
    Descending,
}

impl From<SortOrder> for bool {
    fn from(order: SortOrder) -> Self {
        match order {
            SortOrder::Ascending => true,
            SortOrder::Descending => false,
        }
    }
}

pub async fn logs<T: Log>(query: QueryParams) -> Result<T, query::Error>
where
    query::Error: From<<T as TryFrom<usize>>::Error>,
{
    let limit = query.limit;
    let sort = query.sort.clone().map(|sort| sort.into()).unwrap_or(false);
    let (query, df) = select_logs(query, vec![array_agg(col("key")), array_agg(col("value"))])
        .await
        .and_then(|(q, df)| {
            let df = df.with_column(
                "attributes",
                functions_nested::map::map(
                    vec![col("array_agg(log_attributes.key)")],
                    vec![col("array_agg(log_attributes.value)")],
                ),
            )?;
            Ok((q, df))
        })?;
    if query.map_functions.contains(&ferum_ql::Function::Count) {
        let count = df.count().await?;
        let count = T::try_from(count)?;
        return Ok(count);
    }

    let streams = partition_streams(
        df,
        &[
            "id",
            "array_agg(log_attributes.key)",
            "array_agg(log_attributes.value)",
        ],
        sort,
        limit,
    )
    .await?;

    let should_serialize_message = query.map_functions.contains(&ferum_ql::Function::Json);
    T::try_from_streams(streams, should_serialize_message).await
}

pub async fn attribute_keys<T: FromStreams>(query: QueryParams) -> Result<T, query::Error> {
    let limit = query.limit;
    let sort = query.sort.clone().map(|sort| sort.into()).unwrap_or(false);
    let (_, df) = select_logs(query, vec![array_agg(col("key")).alias("key")]).await?;

    let streams =
        partition_streams(df, &["id", "level", "message", "timestamp"], sort, limit).await?;
    T::try_from_streams(streams, false).await
}

pub async fn attribute_values<T: FromStreams>(
    time_range: &TimeRangeQueryParams,
    attribute: &str,
    limit: Option<usize>,
) -> Result<T, query::Error> {
    let ctx = io::get_sql_context();
    let df = df_from_timeframe(
        ctx,
        tables::log_attributes::NAME,
        &time_range.from,
        &time_range.to,
    )
    .await
    .and_then(|df| {
        df.filter(col("key").eq(lit(attribute)))?
            .select(vec![col("value")])?
            .distinct()?
            .limit(0, Some(limit.unwrap_or(1000)))
    })?;

    let streams = df.execute_stream_partitioned().await?;
    T::try_from_streams(streams, false).await
}

async fn select_logs(
    params: QueryParams,
    aggr_expr: Vec<Expr>,
) -> Result<(ferum_ql::Query, DataFrame), query::Error> {
    let query = ferum_ql::parse(&params.query.unwrap_or("{}".to_string()))
        .map_err(|e| query::Error::Query(e.to_string()))?;

    let ctx = io::get_sql_context();
    let selector = query.selector.clone();
    let time_range = params.time_range;
    let attr_df = df_from_timeframe(
        ctx,
        tables::log_attributes::NAME,
        &time_range.from,
        &time_range.to,
    )
    .await
    .and_then(|df| {
        df.select(vec![
            col("log_id"),
            col("key"),
            col("value"),
            col("day").alias("day2"),
        ])
    })
    .and_then(|mut df| {
        for filter in selector.attributes {
            df = apply_fql_filter(df, "key", ferum_ql::ComparisonOp::Eq, filter.key)
                .and_then(|df| apply_fql_filter(df, "value", filter.op, filter.value))?;
        }
        Ok(df)
    })?;

    let df = df_from_timeframe(ctx, tables::logs::NAME, &time_range.from, &time_range.to)
        .await
        .and_then(|df| apply_optional_filter(df, selector.level))
        .and_then(|df| apply_optional_filter(df, selector.message))
        .and_then(|df| {
            df.join_on(
                attr_df,
                JoinType::Left,
                [col("day").eq(col("day2")), col("log_id").eq(col("id"))],
            )
        })
        .and_then(|df| {
            df.aggregate(
                vec![
                    col("logs.id"),
                    col("logs.level"),
                    col("logs.message"),
                    col("logs.timestamp"),
                ],
                aggr_expr,
            )
        })?;

    Ok((query, df))
}

async fn partition_streams(
    df: DataFrame,
    drop_columns: &[&str],
    sort_asc: bool,
    limit: Option<usize>,
) -> Result<Vec<SendableRecordBatchStream>, DataFusionError> {
    let df = df.drop_columns(drop_columns).and_then(|df| {
        df.sort(vec![col("timestamp").sort(sort_asc, false)])?
            .limit(0, Some(limit.unwrap_or(100)))
    })?;

    df.execute_stream_partitioned().await
}

fn apply_optional_filter(
    df: DataFrame,
    filter: Option<ferum_ql::Filter>,
) -> Result<DataFrame, DataFusionError> {
    if let Some(f) = filter {
        return apply_fql_filter(df, &f.key, f.op, f.value);
    }
    Ok(df)
}

fn apply_fql_filter(
    df: DataFrame,
    column: &str,
    op: ferum_ql::ComparisonOp,
    value: impl Literal,
) -> Result<DataFrame, DataFusionError> {
    let expr = col(column);
    let expr = match op {
        ferum_ql::ComparisonOp::Eq => expr.eq(lit(value)),
        ferum_ql::ComparisonOp::Neq => expr.not_eq(lit(value)),
        ferum_ql::ComparisonOp::Regex => regexp_match(expr, lit(value), None).is_not_null(),
        ferum_ql::ComparisonOp::Greater => expr.gt(lit(value)),
        ferum_ql::ComparisonOp::GreaterEq => expr.gt_eq(lit(value)),
        ferum_ql::ComparisonOp::Less => expr.lt(lit(value)),
        ferum_ql::ComparisonOp::LessEq => expr.lt_eq(lit(value)),
    };

    df.filter(expr)
}

async fn df_from_timeframe(
    ctx: &SessionContext,
    table: &str,
    from: &DateTime<Utc>,
    to: &DateTime<Utc>,
) -> Result<DataFrame, DataFusionError> {
    let today_from = num_days_since_epoch(from);
    let today_to = num_days_since_epoch(to);
    let timestamp_from = lit_timestamp_nano(from.timestamp_nanos_opt().unwrap());
    let timestamp_to = lit_timestamp_nano(to.timestamp_nanos_opt().unwrap());

    let day_filter = if today_from != today_to {
        col("day").between(
            Expr::Literal(ScalarValue::Date32(Some(today_from))),
            Expr::Literal(ScalarValue::Date32(Some(today_to))),
        )
    } else {
        col("day").eq(Expr::Literal(ScalarValue::Date32(Some(today_from))))
    };

    ctx.table(table).await.and_then(|df| {
        df.filter(day_filter.and(col("timestamp").between(timestamp_from, timestamp_to)))
    })
}

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719163;

fn num_days_since_epoch(dt: &DateTime<Utc>) -> i32 {
    dt.date_naive().num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
}
