use crate::io::{get_sql_context, tables};
use crate::server::http::schemas::FqlQueryParams;
use chrono::Datelike;
use chrono::{DateTime, Utc};
use datafusion::common::{JoinType, ScalarValue};
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{col, lit, lit_timestamp_nano, Expr, Literal};
use datafusion::prelude::{regexp_match, SessionContext};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to parse query: {0}")]
    Query(String),

    #[error("{0}")]
    DataFusion(DataFusionError),
}

impl From<DataFusionError> for Error {
    fn from(e: DataFusionError) -> Self {
        Self::DataFusion(e)
    }
}

pub async fn select_logs(
    params: FqlQueryParams,
    aggr_expr: Vec<Expr>,
) -> Result<(ferum_ql::Query, DataFrame), Error> {
    let query = ferum_ql::parse(&params.query.unwrap_or("{}".to_string()))
        .map_err(|e| Error::Query(e.to_string()))?;

    let ctx = get_sql_context();
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

pub async fn partition_streams(
    df: DataFrame,
    drop_columns: &[&str],
    limit: Option<usize>,
) -> Result<Vec<SendableRecordBatchStream>, DataFusionError> {
    let df = df
        .drop_columns(drop_columns)
        .and_then(|df| df.sort(vec![col("timestamp").sort(false, false)]))
        .and_then(|df| df.limit(0, Some(limit.unwrap_or(1000))))?;

    df.execute_stream_partitioned().await
}

pub async fn df_from_timeframe(
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

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719163;

fn num_days_since_epoch(dt: &DateTime<Utc>) -> i32 {
    dt.date_naive().num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
}
