// pub fn init() {
//     tracing_subscriber::init();
// }

// pub mod tracing_subscriber {
//     // - 在log里打印node name
//     //   https://github.com/tokio-rs/tracing/issues/1039
//     //   https://github.com/paritytech/substrate/pull/7328/files#diff-2aff70c0d7f47bf8bc46d493812750120e424ca014752c1ba8b25c03a759a064
//     //

//     use ansi_term::Colour;
//     use ansi_term::Style;
//     use tracing::Level;
//     use tracing::Subscriber;
//     use tracing_subscriber::fmt::time::ChronoUtc;
//     use tracing_subscriber::fmt::time::FormatTime;
//     use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
//     use tracing_subscriber::registry::LookupSpan;
//     use tracing_subscriber::EnvFilter;

//     #[derive(Debug, Clone)]
//     pub struct MyFormatter<T = ChronoUtc> {
//         pub(crate) timer: T,
//     }

//     impl Default for MyFormatter<ChronoUtc> {
//         fn default() -> Self {
//             MyFormatter {
//                 // see https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html
//                 timer: ChronoUtc::with_format("%m-%d %H:%M:%S%.3f".to_string()),
//             }
//         }
//     }

//     impl MyFormatter {
//         pub fn with_timer<T2>(self, timer: T2) -> MyFormatter<T2> {
//             MyFormatter { timer }
//         }
//     }

//     impl<S, N, T> FormatEvent<S, N> for MyFormatter<T>
//     where
//         S: Subscriber + for<'a> LookupSpan<'a>,
//         N: for<'a> FormatFields<'a> + 'static,
//         T: FormatTime,
//     {
//         fn format_event(
//             &self,
//             ctx: &FmtContext<'_, S, N>,
//             writer: &mut dyn std::fmt::Write,
//             event: &tracing::Event<'_>,
//         ) -> std::fmt::Result {
//             #[cfg(feature = "tracing-log")]
//             let normalized_meta = event.normalized_metadata();
//             #[cfg(feature = "tracing-log")]
//             let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());
//             #[cfg(not(feature = "tracing-log"))]
//             let meta = event.metadata();

//             {
//                 let style = Style::new().dimmed();
//                 write!(writer, "{}", style.prefix())?;
//                 FormatTime::format_time(&self.timer, writer)?;
//                 write!(writer, "{} ", style.suffix())?;
//             }
//             //self.format_timestamp(writer)?;

//             match *meta.level() {
//                 Level::TRACE => write!(writer, "{}", Colour::Purple.paint("T"))?,
//                 Level::DEBUG => write!(writer, "{}", Colour::Blue.paint("D"))?,
//                 Level::INFO => write!(writer, "{}", Colour::Green.paint("I"))?,
//                 Level::WARN => write!(writer, "{}", Colour::Yellow.paint("W"))?,
//                 Level::ERROR => write!(writer, "{}", Colour::Red.paint("E"))?,
//             }
//             write!(writer, " ")?;

//             {
//                 // FmtCtx::new(&ctx, event.parent(), self.ansi)
//                 let span_id = event.parent();

//                 let bold = Style::new().bold();
//                 let mut seen = false;

//                 let span = span_id
//                     .and_then(|id| ctx.span(&id))
//                     .or_else(|| ctx.lookup_current());

//                 let scope = span.into_iter().flat_map(|span| span.scope().from_root());

//                 for span in scope {
//                     write!(writer, "{}", bold.paint(span.metadata().name()))?;
//                     seen = true;

//                     let ext = span.extensions();
//                     let fields = &ext
//                         .get::<FormattedFields<N>>()
//                         .expect("Unable to find FormattedFields in extensions; this is a bug");
//                     if !fields.is_empty() {
//                         write!(writer, "{}{}{}", bold.paint("{"), fields, bold.paint("}"))?;
//                     }
//                     writer.write_char(':')?;
//                 }

//                 if seen {
//                     writer.write_char(' ')?;
//                 }
//             }

//             ctx.format_fields(writer, event)?;

//             writeln!(writer)
//         }
//     }

//     fn do_init() {
//         let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV).is_ok() {
//             EnvFilter::from_default_env()
//         } else {
//             EnvFilter::new("info")
//         };

//         tracing_subscriber::fmt()
//             .with_target(false)
//             .with_env_filter(env_filter)
//             .event_format(MyFormatter::default())
//             .init();
//     }

//     pub fn init() {
//         lazy_static::lazy_static!(
//             static ref INITED: () = do_init();
//         );
//         return *INITED;
//     }

//     pub fn init_with_filters(filters: &str) {
//         let mut env_filter = EnvFilter::new(filters);
//         if std::env::var(EnvFilter::DEFAULT_ENV).is_ok() {
//             let df = std::env::var(EnvFilter::DEFAULT_ENV).unwrap();
//             let r = df.split(',');
//             for s in r {
//                 env_filter = env_filter.add_directive(s.parse().unwrap());
//             }
//         }

//         tracing_subscriber::fmt()
//             .with_target(false)
//             .with_env_filter(env_filter)
//             .event_format(MyFormatter::default())
//             .init();
//     }

//     pub fn init_with_span_events(kind: tracing_subscriber::fmt::format::FmtSpan) {
//         let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV).is_ok() {
//             EnvFilter::from_default_env()
//         } else {
//             EnvFilter::new("info")
//         };

//         tracing_subscriber::fmt()
//             .with_target(false)
//             .with_env_filter(env_filter)
//             .with_span_events(kind)
//             .event_format(MyFormatter::default())
//             .init();
//     }
// }


use std::str::FromStr;

use anyhow::Result;
use tracing::metadata::LevelFilter;
use tracing_subscriber::{EnvFilter, fmt::{self, time::{Uptime, LocalTime}}, prelude::*, filter::Directive};
use time::macros::format_description;

pub fn init() -> Result<()> {

    // // see https://time-rs.github.io/book/api/format-description.html
    // let offset = time::UtcOffset::current_local_offset()?;
    // let timer = OffsetTime::new(offset, format_description!("[hour]:[minute]:[second].[subsecond digits:3]"));

    // let layer = fmt::layer()
    // .with_target(true)
    // .with_timer(timer);

    // let filter = EnvFilter::builder()
    // .with_default_directive(LevelFilter::INFO.into())
    // .from_env_lossy();

    // tracing_subscriber::registry()
    // .with(layer)
    // .with(filter)
    // .init();

    // // tracing_subscriber::fmt()
    // // .with_level(display_level)

    // Ok(())


    // init_with_filters("")
    tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .with_max_level(LevelFilter::DEBUG)// 因为在 with_env_filter 后面可以覆盖掉它的配置
    .with_target(false)
    // .with_timer(LocalTime::new(format_description!("[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]")))
    .with_timer(Uptime::default())
    .init();
    Ok(())

}

pub fn init_with_filters(dirs: &str) -> Result<()> {

    // // 会打印不出来时间
    // // see https://time-rs.github.io/book/api/format-description.html
    // let timer = LocalTime::new(format_description!("[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"));

    // // 当在多线程环境下会返回失败
    // let offset = time::UtcOffset::current_local_offset()?;

    // // see https://time-rs.github.io/book/api/format-description.html
    // let timer = OffsetTime::new(offset, format_description!("[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"));

    // see https://time-rs.github.io/book/api/format-description.html
    let timer = LocalTime::new(format_description!("[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"));
    // let timer = tracing_subscriber::fmt::time::LocalTime::new(
    //     format_description!("[month]-[day]T[hour]:[minute]:[second]")
    // );

    let layer = fmt::layer()
    .with_target(false)
    .with_timer(timer);

    let mut filter = EnvFilter::builder()
    .with_default_directive(LevelFilter::INFO.into())
    .parse("")?;

    filter = add_directives(filter, dirs)?;

    filter = {
        let var = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default();
        add_directives(filter, &var)?
    };

    tracing_subscriber::registry()
    .with(layer)
    .with(filter)
    .init();

    Ok(())
}


fn add_directives(mut filter: EnvFilter, dirs: &str) -> Result<EnvFilter> {
    let directives = dirs
    .split(',')
    .filter(|s| !s.is_empty())
    .filter_map(|s| match Directive::from_str(s) {
        Ok(d) => Some(d),
        Err(_err) => {
            // eprintln!("ignoring `{}`: {}", s, err);
            None
        }
    });

    for d in directives {
        filter = filter.add_directive(d);
    }

    Ok(filter)
}

