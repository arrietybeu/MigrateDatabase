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
    backup_before_merge: bool,
    backup_directory: String,
    batch_size: usize,
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
        println!("\n{}", "=== BẮT ĐẦU MERGE 2 SERVER ===".bright_cyan().bold());
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
            println!(
                "\n{} Bạn có muốn tiếp tục merge? (yes/no): ",
                "⚠️".yellow()
            );
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
                    println!(
                        "\n{} Bạn có muốn COMMIT thay đổi? (yes/no): ",
                        "⚠".yellow()
                    );
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

    fn run_merge(&mut self, target_conn: &mut PooledConn, source_conn: &mut PooledConn) -> Result<()> {
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
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'account' AND COLUMN_NAME = 'old_id'"
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
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player' AND COLUMN_NAME = 'old_id'"
        )?;

        if player_has_old_id.is_none() {
            println!("  Tạo cột old_id cho bảng player...");
            if !self.dry_run {
                conn.query_drop("ALTER TABLE player ADD COLUMN old_id INT NULL COMMENT 'ID cũ trước khi merge'")?;
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
        let tables = vec![
            "account",
            "player",
            &clan_table,
            "gift_code_histories",
        ];

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

    fn merge_accounts(&mut self, target_conn: &mut PooledConn, source_conn: &mut PooledConn) -> Result<()> {
        println!("\n{}", ">>> Merge bảng ACCOUNT...".bright_yellow());

        // Lấy tất cả accounts từ Server 2
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
                // Insert vào target với old_id là ID cũ từ server nguồn
                let params = params! {
                    "id" => new_id,
                    "old_id" => old_id,  // Lưu ID cũ
                    "username" => row.get::<String, _>("username").unwrap(),
                    "password" => row.get::<String, _>("password").unwrap(),
                    "active" => row.get::<i32, _>("active").unwrap_or(0),
                    "thoi_vang" => row.get::<i32, _>("thoi_vang").unwrap_or(0),
                    "vnd" => row.get::<i32, _>("vnd").unwrap_or(0),
                    "ban" => row.get::<bool, _>("ban").unwrap_or(false),
                    "ip_address" => row.get::<Option<String>, _>("ip_address"),
                    "last_time_login" => row.get::<Option<String>, _>("last_time_login"),
                    "last_time_logout" => row.get::<Option<String>, _>("last_time_logout"),
                    "is_admin" => row.get::<bool, _>("is_admin").unwrap_or(false),
                    "reward" => row.get::<Option<String>, _>("reward"),
                    "point_nap" => row.get::<i32, _>("pointNap").unwrap_or(0),
                };
                
                target_conn.exec_drop(
                    r"INSERT INTO account 
                    (id, old_id, username, password, active, thoi_vang, vnd, ban, ip_address,
                     last_time_login, last_time_logout, is_admin, reward, `pointNap`)
                    VALUES (:id, :old_id, :username, :password, :active, :thoi_vang, :vnd, :ban,
                            :ip_address, :last_time_login, :last_time_logout, :is_admin, :reward, :point_nap)",
                    params,
                )?;
            }

            pb.inc(1);
        }

        pb.finish_with_message("✓ Hoàn thành");
        println!("{} {} accounts", "✓".green(), total_accounts);
        Ok(())
    }

    fn merge_players(&mut self, target_conn: &mut PooledConn, source_conn: &mut PooledConn) -> Result<()> {
        println!("\n{}", ">>> Merge bảng PLAYER...".bright_yellow());

        // Đơn giản hơn - dùng INSERT ... SELECT với UPDATE ID
        let clan_col = format!("clan_id_sv{}", self.config.merge.target_server);
        let offset = self.config.merge.id_offset;
        
        if !self.dry_run {
            // Tạo temp table rồi UPDATE ID sau
            let sql = format!(
                r#"CREATE TEMPORARY TABLE temp_player AS SELECT * FROM {}.player"#,
                self.config.server2.database
            );
            target_conn.query_drop(&sql)?;
            
            // Thêm cột old_id vào temp table và lưu ID cũ
            target_conn.query_drop("ALTER TABLE temp_player ADD COLUMN old_id INT NULL")?;
            target_conn.query_drop("UPDATE temp_player SET old_id = id")?;

            // Update IDs trong temp table
            target_conn.query_drop(&format!("UPDATE temp_player SET id = id + {}", offset))?;
            target_conn.query_drop(&format!("UPDATE temp_player SET account_id = account_id + {}", offset))?;
            target_conn.query_drop(&format!(
                "UPDATE temp_player SET `{}` = IF(`{}` = -1, -1, `{}` + {})",
                clan_col, clan_col, clan_col, offset
            ))?;
            
            // Insert vào player thật
            target_conn.query_drop("INSERT INTO player SELECT * FROM temp_player")?;
            target_conn.query_drop("DROP TEMPORARY TABLE temp_player")?;
        }

        // Build mapping
        let players: Vec<Row> = source_conn.query("SELECT id FROM player")?;
        let total_players = players.len();
        
        for row in players {
            let old_id: i32 = row.get("id").unwrap();
            let new_id = old_id + offset;
            self.player_mapping.insert(old_id, new_id);
        }

        println!("{} {} players", "✓".green(), total_players);
        Ok(())
    }

    fn merge_clans(&mut self, target_conn: &mut PooledConn, source_conn: &mut PooledConn) -> Result<()> {
        println!("\n{}", ">>> Merge bảng CLAN...".bright_yellow());

        let table_name = format!("clan_sv{}", self.config.merge.target_server);
        let query = format!("SELECT * FROM {}", table_name);
        let clans: Vec<Row> = source_conn.query(&query)?;

        for row in &clans {
            let old_id: i32 = row.get("id").unwrap();
            let new_id = old_id + self.config.merge.id_offset;

            self.clan_mapping.insert(old_id, new_id);

            if !self.dry_run {
                // Update members JSON
                let members_json: String = row.get("members").unwrap();
                let updated_members = self.update_clan_members_json(&members_json)?;

                target_conn.exec_drop(
                    &format!(
                        r"INSERT INTO {} 
                        (id, name, slogan, img_id, power_point, max_member, clan_point, level, members) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        table_name
                    ),
                    (
                        new_id,
                        row.get::<String, _>("name").unwrap_or_default(),
                        row.get::<String, _>("slogan").unwrap_or_default(),
                        row.get::<i32, _>("img_id").unwrap_or(0),
                        row.get::<i64, _>("power_point").unwrap_or(0),
                        row.get::<i8, _>("max_member").unwrap_or(10),
                        row.get::<i32, _>("clan_point").unwrap_or(0),
                        row.get::<i32, _>("level").unwrap_or(1),
                        updated_members,
                    ),
                )?;
            }
        }

        println!("{} {} clans", "✓".green(), clans.len());
        Ok(())
    }

    fn update_clan_members_json(&self, json_str: &str) -> Result<String> {
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

    fn merge_gift_code_histories(&mut self, target_conn: &mut PooledConn, source_conn: &mut PooledConn) -> Result<()> {
        println!("\n{}", ">>> Merge GIFT_CODE_HISTORIES...".bright_yellow());

        let histories: Vec<Row> = source_conn.query("SELECT * FROM gift_code_histories")?;

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
        }

        println!("{} {} gift histories", "✓".green(), histories.len());
        Ok(())
    }

    fn merge_other_tables(&mut self, target_conn: &mut PooledConn, source_conn: &mut PooledConn) -> Result<()> {
        println!("\n{}", ">>> Merge các bảng phụ...".bright_yellow());

        // Merge player_vip
        if let Ok(vips) = source_conn.query::<Row, _>("SELECT * FROM player_vip") {
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
            }
        }

        println!("{} Hoàn thành", "✓".green());
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
                     LIMIT 20"
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
                println!("{}", "Khuyến nghị: Xóa hoặc tạo account cho họ sau khi merge.".yellow());
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
        eprintln!(
            "{} File config không tồn tại: {}",
            "✗".red(),
            args.config
        );
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
