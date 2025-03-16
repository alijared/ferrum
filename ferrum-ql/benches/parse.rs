use criterion::{criterion_group, criterion_main, Criterion};
use ferum_ql::parse;

fn benchmark(c: &mut Criterion) {
    c.bench_function("parse root element only", |b| b.iter(|| { 
        parse(r#"{level="ERROR"}"#)
    }));
    
    c.bench_function("parse root element and attribute", |b| b.iter(|| {
        parse(r#"{message="Hello, World!", target="root"}"#)
    }));

    c.bench_function("parse many attributes", |b| b.iter(|| {
        parse(r#"{level="ERROR", message="Something bad", target="root", http.status_code="404", worker_id="100", pod_name="otelgen-pod-cf186283"}"#)
    }));
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
