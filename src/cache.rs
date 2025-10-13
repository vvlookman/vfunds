use std::{fs::create_dir_all, time::Duration};

use chrono::{Local, NaiveDateTime};
use libsql::{Builder, Connection};

use crate::{CACHE_PATH, error::VfResult};

pub async fn init() -> VfResult<()> {
    if let Some(cache_dir) = CACHE_PATH.parent() {
        create_dir_all(cache_dir)?;
    }

    let conn = connect().await?;
    conn.execute(
        r#"
CREATE TABLE IF NOT EXISTS "cache" (
    "key"     TEXT PRIMARY KEY,
    "data"    BLOB NOT NULL,
    "expire"  TIMESTAMP)
;"#,
        (),
    )
    .await?;

    Ok(())
}

pub async fn get(key: &str, ignore_expire: bool) -> VfResult<Option<Vec<u8>>> {
    let conn = connect().await?;

    let mut rows = conn
        .query(
            r#"
SELECT "data", "expire"
FROM "cache"
WHERE "key" = ?
LIMIT 1
;"#,
            [key],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        let data = row.get::<Vec<u8>>(0)?;

        if ignore_expire {
            return Ok(Some(data));
        } else {
            let expire_str = row.get::<String>(1)?;
            let expire = NaiveDateTime::parse_from_str(&expire_str, "%Y-%m-%d %H:%M:%S")?;
            if expire > Local::now().naive_local() {
                return Ok(Some(data));
            }
        }
    }

    Ok(None)
}

pub async fn upsert(key: &str, data: &[u8], expire: &NaiveDateTime) -> VfResult<()> {
    let expire_str = expire.format("%Y-%m-%d %H:%M:%S").to_string();

    let conn = connect().await?;

    let tx = conn.transaction().await?;
    {
        let exists = tx
            .query(
                r#"
SELECT "expire" 
FROM "cache" 
WHERE "key" = ?
;"#,
                [key],
            )
            .await?
            .next()
            .await?
            .is_some();

        if exists {
            tx.execute(
                r#"
UPDATE "cache"
SET "data" = ?, 
    "expire" = ?
WHERE "key" = ?
;"#,
                (data, expire_str, key),
            )
            .await?;
        } else {
            tx.execute(
                r#"
INSERT INTO "cache" 
    ("key", "data", "expire") 
VALUES 
    (?, ?, ?)
;"#,
                (key, data, expire_str),
            )
            .await?;
        }
    }
    tx.commit().await?;

    Ok(())
}

async fn connect() -> VfResult<Connection> {
    let db = Builder::new_local(&*CACHE_PATH).build().await?;
    let conn = db.connect()?;

    conn.busy_timeout(Duration::from_secs(5))?;
    conn.query("PRAGMA journal_mode=WAL;", ()).await?;

    Ok(conn)
}
