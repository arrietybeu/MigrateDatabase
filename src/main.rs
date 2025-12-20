use anyhow::{Context, Result};
use clap::Parser;
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use log::info;
use mysql::prelude::*;
use mysql::*;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

// ============ Config Structures ============

#[derive(Debug, Deserialize)]
struct Config {
    server1: DatabaseConfig,
    server2: DatabaseConfig,
    merge: MergeConfig,
}

#[derive(Debug, Deserialize)]
struct DatabaseConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct MergeConfig {
    id_offset: i32,
    target_server: u8,
    // backup_before_merge: bool,
    // backup_directory: String,
    // batch_size: usize,
}

// ============ CLI Arguments ============

#[derive(Parser, Debug)]
#[command(name = "DB Merge Tool")]
#[command(about = "Tool merge 2 database game server thành 1", long_about = None)]
struct Args {
    /// Đường dẫn đến file config
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    /// Chế độ dry-run (không thực sự merge, chỉ kiểm tra)
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Bỏ qua backup
    #[arg(long, default_value_t = false)]
    skip_backup: bool,
}

// ============ Main Application ============

struct MergeTool {
    config: Config,
    server1_pool: Pool,
    server2_pool: Pool,
    account_mapping: HashMap<i32, i32>,
    player_mapping: HashMap<i32, i32>,
    clan_mapping: HashMap<i32, i32>,
    dry_run: bool,
}

impl MergeTool {
    fn new(config: Config, dry_run: bool) -> Result<Self> {
        info!("Đang kết nối đến database Server 1...");
        let server1_pool = Self::create_pool(&config.server1)?;

        info!("Đang kết nối đến database Server 2...");
        let server2_pool = Self::create_pool(&config.server2)?;

        Ok(Self {
            config,
            server1_pool,
            server2_pool,
            account_mapping: HashMap::new(),
            player_mapping: HashMap::new(),
            clan_mapping: HashMap::new(),
            dry_run,
        })
    }

    fn create_pool(db_config: &DatabaseConfig) -> Result<Pool> {
        let opts = OptsBuilder::new()
            .ip_or_hostname(Some(&db_config.host))
            .tcp_port(db_config.port)
            .db_name(Some(&db_config.database))
            .user(Some(&db_config.username))
            .pass(Some(&db_config.password));

        Pool::new(opts).context("Không thể kết nối database")
    }

    fn execute(&mut self) -> Result<()> {
        println!(
            "\n{}",
            "=== BẮT ĐẦU MERGE 2 SERVER ===".bright_cyan().bold()
        );
        println!("Server đích: {}", self.config.merge.target_server);
        println!("ID Offset: {}", self.config.merge.id_offset);
        println!(
            "Mode: {}",
            if self.dry_run {
                "DRY RUN (không commit)".yellow()
            } else {
                "PRODUCTION (sẽ commit)".red()
            }
        );
        println!();

        // 1. Thống kê trước merge
        self.print_statistics()?;

        // 2. Xác nhận từ user
        if !self.dry_run {
            println!("\n{} Bạn có muốn tiếp tục merge? (yes/no): ", "⚠️".yellow());
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            if input.trim().to_lowercase() != "yes" {
                println!("Đã hủy merge.");
                return Ok(());
            }
        }

        // 3. Bắt đầu transaction
        let mut server1_conn = self.server1_pool.get_conn()?;
        let mut server2_conn = self.server2_pool.get_conn()?;

        if !self.dry_run {
            server1_conn.query_drop("START TRANSACTION")?;
            server2_conn.query_drop("START TRANSACTION")?;
        }

        // 4. Thực hiện merge
        let result = self.run_merge(&mut server1_conn, &mut server2_conn);

        // 5. Commit hoặc rollback
        match result {
            Ok(_) => {
                if self.dry_run {
                    println!("\n{}", "✓ DRY RUN hoàn thành".green().bold());
                } else {
                    println!("\n{} Bạn có muốn COMMIT thay đổi? (yes/no): ", "⚠".yellow());
                    let mut input = String::new();
                    io::stdin().read_line(&mut input)?;

                    if input.trim().to_lowercase() == "yes" {
                        server1_conn.query_drop("COMMIT")?;
                        server2_conn.query_drop("COMMIT")?;
                        println!("\n{}", "=== MERGE THÀNH CÔNG ===".green().bold());
                    } else {
                        server1_conn.query_drop("ROLLBACK")?;
                        server2_conn.query_drop("ROLLBACK")?;
                        println!("\n{}", "Đã rollback tất cả thay đổi".yellow());
                    }
                }
                Ok(())
            }
            Err(e) => {
                if !self.dry_run {
                    server1_conn.query_drop("ROLLBACK")?;
                    server2_conn.query_drop("ROLLBACK")?;
                }
                Err(e)
            }
        }
    }

    fn run_merge(
        &mut self,
        target_conn: &mut PooledConn,
        source_conn: &mut PooledConn,
    ) -> Result<()> {
        // Tắt foreign key check tạm thời
        target_conn.query_drop("SET FOREIGN_KEY_CHECKS=0")?;

        // Tạo cột old_id nếu chưa có
        self.ensure_old_id_columns(target_conn)?;

        // Merge theo thứ tự
        self.merge_accounts(target_conn, source_conn)?;
        self.merge_players(target_conn, source_conn)?;
        self.merge_clans(target_conn, source_conn)?;
        self.merge_gift_code_histories(target_conn, source_conn)?;
        self.merge_other_tables(target_conn, source_conn)?;

        // Bật lại foreign key check
        target_conn.query_drop("SET FOREIGN_KEY_CHECKS=1")?;

        // Verify
        self.verify_merge(target_conn)?;

        Ok(())
    }

    fn ensure_old_id_columns(&self, conn: &mut PooledConn) -> Result<()> {
        println!("\n{}", ">>> Kiểm tra và tạo cột old_id...".bright_yellow());

        // Kiểm tra và thêm cột old_id cho bảng account
        let account_has_old_id: Option<String> = conn.query_first(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'account' AND COLUMN_NAME = 'old_id'",
        )?;

        if account_has_old_id.is_none() {
            println!("  Tạo cột old_id cho bảng account...");
            if !self.dry_run {
                conn.query_drop("ALTER TABLE account ADD COLUMN old_id INT NULL COMMENT 'ID cũ trước khi merge'")?;
            }
            println!("{} Đã tạo cột old_id cho bảng account", "✓".green());
        } else {
            println!("{} Cột old_id đã tồn tại trong bảng account", "✓".green());
        }

        // Kiểm tra và thêm cột old_id cho bảng player
        let player_has_old_id: Option<String> = conn.query_first(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player' AND COLUMN_NAME = 'old_id'",
        )?;

        if player_has_old_id.is_none() {
            println!("  Tạo cột old_id cho bảng player...");
            if !self.dry_run {
                conn.query_drop(
                    "ALTER TABLE player ADD COLUMN old_id INT NULL COMMENT 'ID cũ trước khi merge'",
                )?;
            }
            println!("{} Đã tạo cột old_id cho bảng player", "✓".green());
        } else {
            println!("{} Cột old_id đã tồn tại trong bảng player", "✓".green());
        }

        Ok(())
    }

    fn print_statistics(&mut self) -> Result<()> {
        println!("\n{}", "=== THỐNG KÊ TRƯỚC KHI MERGE ===".bright_cyan());

        let mut server1_conn = self.server1_pool.get_conn()?;
        let mut server2_conn = self.server2_pool.get_conn()?;

        let clan_table = format!("clan_sv{}", self.config.merge.target_server);
        let tables = vec!["account", "player", &clan_table, "gift_code_histories"];

        for table in tables {
            let count1 = self.get_row_count(&mut server1_conn, table)?;
            let count2 = self.get_row_count(&mut server2_conn, table)?;
            println!(
                "{:<25} | Server1: {:>6} | Server2: {:>6} | Tổng: {:>6}",
                table,
                count1,
                count2,
                count1 + count2
            );
        }

        println!("{}", "=".repeat(80));
        Ok(())
    }

    fn get_row_count(&self, conn: &mut PooledConn, table: &str) -> Result<i64> {
        let query = format!("SELECT COUNT(*) FROM {}", table);
        let count: Option<i64> = conn.query_first(&query)?;
        Ok(count.unwrap_or(0))
    }

    // Helper function để đọc BIT(1) từ MySQL
    fn get_bit_as_bool(row: &Row, col: &str) -> Option<bool> {
        // BIT(1) có thể trả về dạng bytes hoặc i8
        if let Some(val) = row.get_opt::<Value, _>(col) {
            match val {
                Ok(Value::Bytes(bytes)) => {
                    if bytes.is_empty() {
                        Some(false)
                    } else {
                        Some(bytes[0] != 0)
                    }
                }
                Ok(Value::Int(i)) => Some(i != 0),
                Ok(Value::UInt(u)) => Some(u != 0),
                Ok(Value::NULL) => None,
                _ => None,
            }
        } else {
            None
        }
    }

    fn merge_accounts(
        &mut self,
        target_conn: &mut PooledConn,
        source_conn: &mut PooledConn,
    ) -> Result<()> {
        println!("\n{}", ">>> Merge bảng ACCOUNT...".bright_yellow());

        let accounts: Vec<Row> = source_conn.query("SELECT * FROM account")?;

        let pb = ProgressBar::new(accounts.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap(),
        );

        let total_accounts = accounts.len();

        for row in accounts {
            let old_id: i32 = row.get("id").unwrap();
            let new_id = old_id + self.config.merge.id_offset;

            // Lưu mapping
            self.account_mapping.insert(old_id, new_id);

            if !self.dry_run {
                // Xử lý các cột BIT(1) đặc biệt
                let is_daily = Self::get_bit_as_bool(&row, "is_daily");
                let is_admin_bit = Self::get_bit_as_bool(&row, "isAdmin");

                let params: Vec<Value> = vec![
                    Value::from(new_id),
                    Value::from(old_id),
                    Value::from(row.get::<String, _>("username").unwrap()),
                    Value::from(row.get::<String, _>("password").unwrap()),
                    row.get::<Value, _>("create_time").unwrap_or(Value::NULL),
                    row.get::<Value, _>("update_time").unwrap_or(Value::NULL),
                    Value::from(row.get::<i16, _>("ban").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("point_post").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("last_post").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("role").unwrap_or(-1)),
                    Value::from(row.get::<i8, _>("is_admin").unwrap_or(0)),
                    row.get::<Value, _>("last_time_login")
                        .unwrap_or(Value::NULL),
                    row.get::<Value, _>("last_time_logout")
                        .unwrap_or(Value::NULL),
                    row.get::<Value, _>("ip_address").unwrap_or(Value::NULL),
                    Value::from(row.get::<i32, _>("active").unwrap_or(0)),
                    row.get::<Value, _>("reward").unwrap_or(Value::NULL),
                    Value::from(row.get::<i32, _>("thoi_vang").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("server_login").unwrap_or(1)),
                    Value::from(row.get::<i32, _>("new_reg").unwrap_or(0)),
                    row.get::<Value, _>("ip").unwrap_or(Value::NULL),
                    row.get::<Value, _>("phone").unwrap_or(Value::NULL),
                    row.get::<Value, _>("last_server_change_time")
                        .unwrap_or(Value::NULL),
                    Value::from(row.get::<i32, _>("ruby").unwrap_or(0)),
                    row.get::<Value, _>("count_card").unwrap_or(Value::NULL),
                    row.get::<Value, _>("type_bonus").unwrap_or(Value::NULL),
                    row.get::<Value, _>("ref").unwrap_or(Value::NULL),
                    Value::from(row.get::<i32, _>("diemgioithieu").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("vnd_old").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("tongnap_old").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("gioithieu").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("tongnap").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("account_old").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("pointNap").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("vnd").unwrap_or(0)),
                    Value::from(row.get::<i32, _>("tongnapcu").unwrap_or(0)),
                    match is_daily {
                        Some(b) => Value::from(b),
                        None => Value::NULL,
                    },
                    row.get::<Value, _>("money").unwrap_or(Value::NULL),
                    match is_admin_bit {
                        Some(b) => Value::from(b),
                        None => Value::NULL,
                    },
                    row.get::<Value, _>("purchasedGifts").unwrap_or(Value::NULL),
                    row.get::<Value, _>("claimed_accumulate")
                        .unwrap_or(Value::NULL),
                    row.get::<Value, _>("ip_address_register")
                        .unwrap_or(Value::NULL),
                ];

                target_conn.exec_drop(
                    r"INSERT INTO account
                (`id`, `old_id`, `username`, `password`, `create_time`, `update_time`, `ban`, `point_post`, `last_post`,
                 `role`, `is_admin`, `last_time_login`, `last_time_logout`, `ip_address`, `active`, `reward`,
                 `thoi_vang`, `server_login`, `new_reg`, `ip`, `phone`, `last_server_change_time`, `ruby`,
                 `count_card`, `type_bonus`, `ref`, `diemgioithieu`, `vnd_old`, `tongnap_old`, `gioithieu`,
                 `tongnap`, `account_old`, `pointNap`, `vnd`, `tongnapcu`, `is_daily`, `money`, `isAdmin`,
                 `purchasedGifts`, `claimed_accumulate`, `ip_address_register`)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?)",
                    Params::Positional(params),
                )?;
            }

            pb.inc(1);
        }

        pb.finish_with_message("✓ Hoàn thành");
        println!("{} {} accounts", "✓".green(), total_accounts);
        Ok(())
    }

    fn merge_players(
        &mut self,
        target_conn: &mut PooledConn,
        source_conn: &mut PooledConn,
    ) -> Result<()> {
        println!("\n{}", ">>> Merge bảng PLAYER...".bright_yellow());

        let clan_col = format!("clan_id_sv{}", self.config.merge.target_server);
        let offset = self.config.merge.id_offset;

        // Build mapping trước
        let players: Vec<Row> = source_conn.query("SELECT id FROM player")?;
        let total_players = players.len();

        let pb = ProgressBar::new(total_players as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap(),
        );
        pb.set_message("Building mapping...");

        for row in &players {
            let old_id: i32 = row.get("id").unwrap();
            let new_id = old_id + offset;
            self.player_mapping.insert(old_id, new_id);
            pb.inc(1);
        }

        if !self.dry_run {
            pb.set_message("Đang tạo temp table...");
            // Lấy danh sách cột của bảng player (trừ old_id)
            let columns: Vec<String> = target_conn.query(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player'
                 AND COLUMN_NAME != 'old_id'
                 ORDER BY ORDINAL_POSITION",
            )?;

            // Tạo danh sách cột với backticks cho các cột đặc biệt
            let columns_escaped: Vec<String> = columns.iter().map(|c| format!("`{}`", c)).collect();
            let columns_str = columns_escaped.join(", ");

            // Tạo temp table với cấu trúc giống hệt (không có old_id)
            let sql = format!(
                "CREATE TEMPORARY TABLE temp_player AS SELECT {} FROM {}.player",
                columns_str, self.config.server2.database
            );
            target_conn.query_drop(&sql)?;

            pb.set_message("Đang update IDs...");
            // Update IDs trong temp table
            target_conn.query_drop(&format!("UPDATE temp_player SET `id` = `id` + {}", offset))?;
            target_conn.query_drop(&format!(
                "UPDATE temp_player SET `account_id` = `account_id` + {} WHERE `account_id` IS NOT NULL",
                offset
            ))?;
            target_conn.query_drop(&format!(
                "UPDATE temp_player SET `{}` = `{}` + {} WHERE `{}` != -1",
                clan_col, clan_col, offset, clan_col
            ))?;

            // Thêm cột old_id vào temp table và tính giá trị
            target_conn.query_drop("ALTER TABLE temp_player ADD COLUMN `old_id` INT NULL")?;
            target_conn.query_drop(&format!(
                "UPDATE temp_player SET `old_id` = `id` - {}",
                offset
            ))?;

            pb.set_message("Đang insert vào player...");
            // Insert với chỉ định rõ các cột
            let insert_columns = format!("{}, `old_id`", columns_str);
            let select_columns = format!("{}, `old_id`", columns_str);

            target_conn.query_drop(&format!(
                "INSERT INTO player ({}) SELECT {} FROM temp_player",
                insert_columns, select_columns
            ))?;

            target_conn.query_drop("DROP TEMPORARY TABLE temp_player")?;
        }

        pb.finish_with_message("✓ Hoàn thành");
        println!("{} {} players", "✓".green(), total_players);
        Ok(())
    }

    fn merge_clans(
        &mut self,
        target_conn: &mut PooledConn,
        source_conn: &mut PooledConn,
    ) -> Result<()> {
        println!("\n{}", ">>> Merge bảng CLAN...".bright_yellow());

        let table_name = format!("clan_sv{}", self.config.merge.target_server);
        let offset = self.config.merge.id_offset;

        // Build mapping trước
        let query = format!("SELECT id FROM {}", table_name);
        let clans: Vec<Row> = source_conn.query(&query)?;
        let total_clans = clans.len();

        let pb = ProgressBar::new(total_clans as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap(),
        );
        pb.set_message("Building mapping...");

        for row in &clans {
            let old_id: i32 = row.get("id").unwrap();
            let new_id = old_id + offset;
            self.clan_mapping.insert(old_id, new_id);
            pb.inc(1);
        }

        if !self.dry_run {
            pb.set_position(0);
            pb.set_message("Đang tạo temp table...");

            // Lấy danh sách cột của bảng clan
            let columns: Vec<String> = target_conn.query(&format!(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'
                     ORDER BY ORDINAL_POSITION",
                table_name
            ))?;

            // Tạo danh sách cột với backticks
            let columns_escaped: Vec<String> = columns.iter().map(|c| format!("`{}`", c)).collect();
            let columns_str = columns_escaped.join(", ");

            // Tạo temp table với cấu trúc giống hệt
            let sql = format!(
                "CREATE TEMPORARY TABLE temp_clan AS SELECT {} FROM {}.{}",
                columns_str, self.config.server2.database, table_name
            );
            target_conn.query_drop(&sql)?;

            pb.set_message("Đang update IDs...");
            // Update IDs trong temp table
            target_conn.query_drop(&format!("UPDATE temp_clan SET `id` = `id` + {}", offset))?;

            // Update members JSON - cập nhật player_id trong JSON
            pb.set_message("Đang update members JSON...");
            let temp_clans: Vec<Row> =
                target_conn.query("SELECT `id`, `members` FROM temp_clan")?;

            for row in &temp_clans {
                let clan_id: i32 = row.get("id").unwrap();
                let members_json: String = row.get("members").unwrap_or_default();

                if !members_json.is_empty() {
                    let updated_members = self.update_clan_members_json(&members_json)?;
                    target_conn.exec_drop(
                        "UPDATE temp_clan SET `members` = ? WHERE `id` = ?",
                        (&updated_members, clan_id),
                    )?;
                }
                pb.inc(1);
            }

            pb.set_message("Đang insert vào clan...");
            // Insert vào bảng chính
            target_conn.query_drop(&format!(
                "INSERT INTO {} ({}) SELECT {} FROM temp_clan",
                table_name, columns_str, columns_str
            ))?;

            target_conn.query_drop("DROP TEMPORARY TABLE temp_clan")?;
        }

        pb.finish_with_message("✓ Hoàn thành");
        println!("{} {} clans", "✓".green(), total_clans);
        Ok(())
    }

    fn update_clan_members_json(&self, json_str: &str) -> Result<String> {
        // Parse outer array
        let members_raw: Vec<JsonValue> = serde_json::from_str(json_str)?;
        let mut updated_members: Vec<String> = Vec::new();

        for member_value in &members_raw {
            // Mỗi member có thể là string chứa JSON hoặc object trực tiếp
            let member_str = match member_value {
                JsonValue::String(s) => s.clone(),
                other => other.to_string(),
            };

            // Parse member JSON string thành object
            let mut member_obj: serde_json::Map<String, JsonValue> =
                serde_json::from_str(&member_str)?;

            // Update player id từ old_id sang new_id
            if let Some(id_value) = member_obj.get("id") {
                if let Some(old_id) = id_value.as_i64() {
                    let old_id_i32 = old_id as i32;
                    if let Some(&new_id) = self.player_mapping.get(&old_id_i32) {
                        member_obj.insert("id".to_string(), JsonValue::from(new_id));
                        info!("Updated member id: {} -> {}", old_id_i32, new_id);
                    }
                }
            }

            // Convert back to string
            let updated_member_str = serde_json::to_string(&member_obj)?;

            // Giữ nguyên format gốc
            if member_value.is_string() {
                // Format gốc là array of strings
                updated_members.push(updated_member_str);
            } else {
                // Format gốc là array of objects - return early
                return self.update_clan_members_json_as_objects(json_str);
            }
        }

        // Rebuild array of strings
        let result: Vec<JsonValue> = updated_members.into_iter().map(JsonValue::String).collect();

        Ok(serde_json::to_string(&result)?)
    }

    // Fallback cho trường hợp format là array of objects
    fn update_clan_members_json_as_objects(&self, json_str: &str) -> Result<String> {
        let mut members: Vec<JsonValue> = serde_json::from_str(json_str)?;

        for member in &mut members {
            if let Some(obj) = member.as_object_mut() {
                if let Some(id) = obj.get("id").and_then(|v| v.as_i64()) {
                    let old_id = id as i32;
                    if let Some(&new_id) = self.player_mapping.get(&old_id) {
                        obj.insert("id".to_string(), JsonValue::from(new_id));
                    }
                }
            }
        }

        Ok(serde_json::to_string(&members)?)
    }

    fn merge_gift_code_histories(
        &mut self,
        target_conn: &mut PooledConn,
        source_conn: &mut PooledConn,
    ) -> Result<()> {
        println!("\n{}", ">>> Merge GIFT_CODE_HISTORIES...".bright_yellow());

        let histories: Vec<Row> = source_conn.query("SELECT * FROM gift_code_histories")?;
        let total_histories = histories.len();

        let pb = ProgressBar::new(total_histories as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap(),
        );

        for row in &histories {
            let old_player_id: i32 = row.get("player_id").unwrap();
            let new_player_id = self
                .player_mapping
                .get(&old_player_id)
                .copied()
                .unwrap_or(old_player_id);

            if !self.dry_run {
                target_conn.exec_drop(
                    r"INSERT INTO gift_code_histories
                    (player_id, gift_code_id, code, type_clone, created_at)
                    VALUES (?, ?, ?, ?, ?)",
                    (
                        new_player_id,
                        row.get::<i32, _>("gift_code_id").unwrap(),
                        row.get::<String, _>("code").unwrap(),
                        row.get::<i32, _>("type_clone").unwrap_or(-1),
                        row.get::<Option<String>, _>("created_at"),
                    ),
                )?;
            }

            pb.inc(1);
        }

        pb.finish_with_message("✓ Hoàn thành");
        println!("{} {} gift histories", "✓".green(), total_histories);
        Ok(())
    }

    fn merge_other_tables(
        &mut self,
        target_conn: &mut PooledConn,
        source_conn: &mut PooledConn,
    ) -> Result<()> {
        println!("\n{}", ">>> Merge các bảng phụ...".bright_yellow());

        // Merge player_vip
        if let Ok(vips) = source_conn.query::<Row, _>("SELECT * FROM player_vip") {
            let total_vips = vips.len();
            let pb = ProgressBar::new(total_vips as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} player_vip")
                    .unwrap(),
            );

            for row in vips {
                let old_player_id: i32 = row.get("player_id").unwrap();
                let new_player_id = self
                    .player_mapping
                    .get(&old_player_id)
                    .copied()
                    .unwrap_or(old_player_id);

                if !self.dry_run {
                    target_conn.exec_drop(
                        "INSERT INTO player_vip (player_id, vip_1, vip_2) VALUES (?, ?, ?)",
                        (
                            new_player_id,
                            row.get::<bool, _>("vip_1").unwrap_or(false),
                            row.get::<bool, _>("vip_2").unwrap_or(false),
                        ),
                    )?;
                }

                pb.inc(1);
            }

            pb.finish_with_message("✓ Hoàn thành");
            println!("{} {} player_vip records", "✓".green(), total_vips);
        }

        println!("{} Hoàn thành merge bảng phụ", "✓".green());
        Ok(())
    }

    fn verify_merge(&self, conn: &mut PooledConn) -> Result<()> {
        println!("\n{}", "=== VERIFY KẾT QUẢ ===".bright_cyan());

        // Check player without account
        let orphan_players: Option<i64> = conn.query_first(
            "SELECT COUNT(*) FROM player p
             LEFT JOIN account a ON p.account_id = a.id
             WHERE a.id IS NULL",
        )?;

        if let Some(count) = orphan_players {
            if count > 0 {
                println!("{} {} players không có account!", "⚠".yellow(), count);

                // Log chi tiết các player orphan
                let orphans: Vec<(i32, String, i32)> = conn.query(
                    "SELECT p.id, p.name, p.account_id FROM player p
                     LEFT JOIN account a ON p.account_id = a.id
                     WHERE a.id IS NULL
                     LIMIT 20",
                )?;

                println!("\n{}", "Chi tiết players không có account:".yellow());
                println!("{:<10} {:<30} {:<15}", "ID", "Name", "Account_ID");
                println!("{}", "-".repeat(60));
                for (id, name, acc_id) in orphans {
                    println!("{:<10} {:<30} {:<15}", id, name, acc_id);
                }

                if count > 20 {
                    println!("\n... và {} players khác", count - 20);
                }

                println!("\n{}", "Các players này sẽ KHÔNG thể login được!".red());
                println!(
                    "{}",
                    "Khuyến nghị: Xóa hoặc tạo account cho họ sau khi merge.".yellow()
                );
            } else {
                println!("{} Tất cả players đều có account", "✓".green());
            }
        }

        println!("{}", "=".repeat(80));
        Ok(())
    }
}

// ============ Main Function ============

fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    // Load config
    let config_path = Path::new(&args.config);
    if !config_path.exists() {
        eprintln!("{} File config không tồn tại: {}", "✗".red(), args.config);
        eprintln!("Hãy copy config.toml thành config.production.toml và cấu hình");
        std::process::exit(1);
    }

    let timer = std::time::Instant::now();

    let config_str = fs::read_to_string(config_path)?;
    let config: Config = toml::from_str(&config_str)?;

    // Tạo tool và chạy
    let mut tool = MergeTool::new(config, args.dry_run)?;
    tool.execute()?;

    let duration = timer.elapsed();
    println!("Thời gian chạy: {}s", duration.as_secs());

    Ok(())
}
