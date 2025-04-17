use crate::{Error, FromRecordBatch};
use chrono::Datelike;
use chrono::{DateTime, TimeDelta, Utc};
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::dataframe;
use datafusion::logical_expr::{Expr, JoinType, Literal, SortExpr, lit, lit_timestamp_nano};
use datafusion::prelude::{SessionContext, col, regexp_match};
use futures::TryStreamExt;
use std::ops::Sub;

pub struct Builder<'a> {
    ctx: &'a SessionContext,
    time_range: TimeRange,
    sort: Vec<(&'a str, bool)>,
    limit: usize,
}

impl<'a> Builder<'a> {
    pub fn new(ctx: &'a SessionContext, timestamp_column: impl Into<Column>) -> Self {
        let now = Utc::now();
        Self {
            ctx,
            time_range: TimeRange {
                timestamp_column: col(timestamp_column),
                partition_column: None,
                from: now.sub(TimeDelta::hours(1)),
                to: now,
            },
            sort: Vec::new(),
            limit: 1000,
        }
    }

    #[inline]
    pub fn with_timeframe(
        mut self,
        partition_column: Option<impl Into<Column>>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Self {
        self.time_range = TimeRange {
            timestamp_column: self.time_range.timestamp_column,
            partition_column: partition_column.map(|c| col(c)),
            from,
            to,
        };

        self
    }

    #[inline]
    pub fn with_sort(mut self, sort: Vec<(&'a str, bool)>) -> Self {
        self.sort = sort;

        self
    }

    #[inline]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;

        self
    }

    pub async fn query(self, table: impl Into<TableReference>) -> Result<DataFrame, Error> {
        let df = self
            .ctx
            .table(table)
            .await?
            .filter(self.time_range.into())?;

        let mut sort = Vec::with_capacity(self.sort.len());
        for c in self.sort {
            sort.push(SortExpr {
                expr: col(c.0),
                asc: c.1,
                nulls_first: false,
            })
        }

        Ok(DataFrame::new(df, sort, self.limit))
    }
}

struct TimeRange {
    timestamp_column: Expr,
    partition_column: Option<Expr>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

impl From<TimeRange> for Expr {
    fn from(range: TimeRange) -> Self {
        let ts_filter = range.timestamp_column.between(
            lit_timestamp_nano(range.from.timestamp_nanos_opt().unwrap()),
            lit_timestamp_nano(range.to.timestamp_nanos_opt().unwrap()),
        );

        match range.partition_column {
            Some(col) => col
                .between(
                    Expr::Literal(ScalarValue::Date32(Some(num_days_since_epoch(&range.from)))),
                    Expr::Literal(ScalarValue::Date32(Some(num_days_since_epoch(&range.to)))),
                )
                .and(ts_filter),
            None => ts_filter,
        }
    }
}

pub struct DataFrame {
    inner: dataframe::DataFrame,
    sort: Vec<SortExpr>,
    limit: usize,
}

impl DataFrame {
    fn new(df: dataframe::DataFrame, sort: Vec<SortExpr>, limit: usize) -> Self {
        Self {
            inner: df,
            sort,
            limit,
        }
    }

    pub fn filter(mut self, predicate: Expr) -> Result<Self, Error> {
        self.inner = self.inner.filter(predicate)?;

        Ok(self)
    }

    pub fn join(
        mut self,
        right: Self,
        join_type: JoinType,
        on_exprs: impl IntoIterator<Item = Expr>,
    ) -> Result<Self, Error> {
        self.inner = self.inner.join_on(right.inner, join_type, on_exprs)?;

        Ok(self)
    }

    pub fn aggregate(mut self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self, Error> {
        self.inner = self.inner.aggregate(group_expr, aggr_expr)?;

        Ok(self)
    }

    pub fn with_column(mut self, name: &str, expr: Expr) -> Result<Self, Error> {
        self.inner = self.inner.with_column(name, expr)?;

        Ok(self)
    }

    pub fn drop_columns(mut self, columns: &[&str]) -> Result<Self, Error> {
        self.inner = self.inner.drop_columns(columns)?;

        Ok(self)
    }

    pub async fn count(self) -> Result<usize, Error> {
        self.inner.count().await.map_err(Into::into)
    }

    pub async fn collect<T: FromRecordBatch>(self) -> Result<Vec<T>, Error> {
        let df = self.inner.sort(self.sort)?.limit(0, Some(self.limit))?;
        let streams = df.execute_stream_partitioned().await?;
        let mut rows = Vec::new();
        for mut stream in streams {
            while let Some(batch) = stream.try_next().await? {
                let mut v = T::from_batch(&batch);
                rows.append(&mut v);
            }
        }

        Ok(rows)
    }
}

pub fn parse_query(query: &str) -> Result<ferrum_ql::Query, Error> {
    ferrum_ql::parse(query).map_err(Into::into)
}

#[inline]
pub fn lit_expr(column: &str, op: ferrum_ql::ComparisonOp, value: impl Literal) -> Expr {
    let expr = col(column);
    match op {
        ferrum_ql::ComparisonOp::Eq => expr.eq(lit(value)),
        ferrum_ql::ComparisonOp::Neq => expr.not_eq(lit(value)),
        ferrum_ql::ComparisonOp::Regex => regexp_match(expr, lit(value), None).is_not_null(),
        ferrum_ql::ComparisonOp::Greater => expr.gt(lit(value)),
        ferrum_ql::ComparisonOp::GreaterEq => expr.gt_eq(lit(value)),
        ferrum_ql::ComparisonOp::Less => expr.lt(lit(value)),
        ferrum_ql::ComparisonOp::LessEq => expr.lt_eq(lit(value)),
    }
}

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719163;

#[inline]
fn num_days_since_epoch(dt: &DateTime<Utc>) -> i32 {
    dt.date_naive().num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE
}
