//! # Backend Comparison Example
//! 
//! Demonstrates using both emulator and AWS backends with the same DDL

use lakesql_core::*;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🦀 LakeSQL Backend Comparison Demo\n");

    // Test DDL statements
    let ddl_statements = vec![
        "CREATE TAG department VALUES ('finance', 'engineering', 'marketing')",
        "GRANT SELECT ON DATABASE sales TO ROLE data_scientist",
        "GRANT SELECT, INSERT ON sales.orders TO USER 'alice@company.com'",
        "CREATE ROLE analytics_team",
    ];

    println!("📝 Testing DDL Statements:");
    for sql in &ddl_statements {
        println!("   {}", sql);
    }
    println!();

    // Test with emulator backend (Airplane Mode)
    println!("✈️  Testing with Emulator Backend (Airplane Mode)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let emulator_config = BackendConfig::Emulator {
        state_file: Some("/tmp/lakesql_demo.json".to_string()),
    };

    match test_backend(emulator_config, &ddl_statements).await {
        Ok(_) => println!("✅ Emulator backend test completed successfully!\n"),
        Err(e) => println!("❌ Emulator backend test failed: {}\n", e),
    }

    // Test with AWS backend (real Lake Formation)
    println!("☁️  Testing with AWS Backend (Real Lake Formation)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("⚠️  Note: This requires valid AWS credentials and permissions");

    let aws_config = BackendConfig::Aws {
        region: Some("us-east-1".to_string()),
        profile: None,
        endpoint: None, // Use None for real AWS, or Some(url) for LocalStack
    };

    match test_backend(aws_config, &ddl_statements).await {
        Ok(_) => println!("✅ AWS backend test completed successfully!"),
        Err(e) => println!("⚠️  AWS backend test skipped/failed: {}\n   {}", e, 
                          "   This is expected without proper AWS credentials"),
    }

    println!("\n🎯 Summary:");
    println!("   • Emulator backend: Perfect for development and testing");
    println!("   • AWS backend: Production-ready Lake Formation integration");
    println!("   • Same DDL syntax works with both backends!");

    Ok(())
}

async fn test_backend(config: BackendConfig, statements: &[&str]) -> Result<()> {
    let mut backend = BackendFactory::create(config).await?;

    for (i, sql) in statements.iter().enumerate() {
        println!("   {}. Executing: {}", i + 1, sql);
        
        match backend.execute_ddl(sql).await {
            Ok(result) => {
                println!("      ✅ {}", result);
            }
            Err(e) => {
                println!("      ❌ Error: {}", e);
                return Err(e);
            }
        }
    }

    // Test permission checks
    println!("   🔍 Testing permission checks...");
    
    let user_alice = Principal::User("alice@company.com".to_string());
    let sales_orders = Resource::Table {
        database: "sales".to_string(),
        table: "orders".to_string(),
        columns: None,
    };

    let has_select = backend.check_permissions(&user_alice, &sales_orders, &Action::Select).await?;
    println!("      Alice has SELECT on sales.orders: {}", has_select);

    let has_delete = backend.check_permissions(&user_alice, &sales_orders, &Action::Delete).await?;
    println!("      Alice has DELETE on sales.orders: {}", has_delete);

    Ok(())
}