use lakesql_core::*;
use lakesql_emulator::EmulatorBackend;
use clap::{Parser, Subcommand};
use anyhow::Result;
use std::collections::HashMap;

#[derive(Parser)]
#[command(name = "lakesql")]
#[command(about = "Lake Formation DDL emulator and testing tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(short, long)]
    /// State file for persistence (optional)
    state_file: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute DDL statements interactively
    Execute {
        /// DDL statement to execute
        #[arg(short, long)]
        sql: Option<String>,
    },
    /// Run comprehensive demo
    Demo,
    /// Run row-level security demo
    RowDemo,
    /// Check permissions
    Check {
        /// Principal (e.g., "ROLE analyst" or "USER john@company.com")
        #[arg(short, long)]
        principal: String,
        /// Resource (e.g., "sales.orders" or "DATABASE sales")  
        #[arg(short, long)]
        resource: String,
        /// Action to check
        #[arg(short, long)]
        action: String,
    },
    /// Show current state
    Status,
    /// Export state
    Export {
        #[arg(short, long)]
        format: Option<String>, // "sql" or "summary"
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut backend = EmulatorBackend::new(cli.state_file).await?;

    match cli.command {
        Commands::Execute { sql } => {
            if let Some(sql_stmt) = sql {
                execute_statement(&mut backend, &sql_stmt).await?;
            } else {
                println!("🎯 Interactive DDL mode not implemented yet");
                println!("💡 Use: lakesql execute --sql \"CREATE ROLE analyst\"");
            }
        },
        
        Commands::Demo => {
            run_demo(&mut backend).await?;
        },

        Commands::RowDemo => {
            run_row_level_security_demo(&mut backend).await?;
        },
        
        Commands::Check { principal, resource, action } => {
            check_permission(&backend, &principal, &resource, &action).await?;
        },
        
        Commands::Status => {
            show_status(&backend).await?;
        },
        
        Commands::Export { format } => {
            export_state(&backend, format.as_deref().unwrap_or("summary")).await?;
        },
    }

    Ok(())
}

async fn execute_statement(backend: &mut EmulatorBackend, sql: &str) -> Result<()> {
    println!("🔧 Executing: {}", sql);
    
    match backend.execute_ddl(sql).await {
        Ok(result) => {
            match result {
                DdlResult::Success { message } => {
                    println!("✅ Success: {}", message);
                },
                DdlResult::Error { error } => {
                    println!("❌ Error: {}", error);
                },
                DdlResult::PermissionCheck { allowed, reason } => {
                    println!("🔍 Permission Check: {} ({})", 
                        if allowed { "ALLOWED" } else { "DENIED" }, 
                        reason.unwrap_or_default()
                    );
                },
            }
        },
        Err(e) => {
            println!("❌ Execution failed: {}", e);
        }
    }
    
    Ok(())
}

async fn run_demo(backend: &mut EmulatorBackend) -> Result<()> {
    println!("🦀 Lake Formation DDL Demo 🦀\n");
    println!("Building a complete data access control scenario...\n");

    let statements = vec![
        // Create roles
        ("📝 Creating roles...", vec![
            "CREATE ROLE data_scientist",
            "CREATE ROLE analyst", 
            "CREATE ROLE intern",
            "CREATE ROLE admin",
        ]),
        
        // Create tags
        ("🏷️  Creating tags...", vec![
            "CREATE TAG department VALUES ('finance', 'marketing', 'engineering', 'hr')",
            "CREATE TAG classification VALUES ('public', 'internal', 'confidential', 'restricted')",
        ]),
        
        // Grant database-level permissions
        ("🗄️  Granting database permissions...", vec![
            "GRANT DESCRIBE ON DATABASE sales TO ROLE analyst",
            "GRANT CREATE_TABLE, DROP_TABLE ON DATABASE analytics TO ROLE admin",
        ]),
        
        // Grant table-level permissions
        ("📊 Granting table permissions...", vec![
            "GRANT SELECT, INSERT ON sales.orders TO ROLE data_scientist",
            "GRANT SELECT ON sales.orders TO ROLE analyst", 
            "GRANT SELECT ON sales.customers TO ROLE analyst",
        ]),
        
        // Grant column-level permissions  
        ("🔒 Granting column-level permissions...", vec![
            "GRANT SELECT ON hr.employees TO ROLE intern",
        ]),
        
        // Revoke some permissions
        ("❌ Revoking permissions...", vec![
            "REVOKE INSERT ON sales.orders FROM ROLE intern",
        ]),
    ];

    for (stage, stage_statements) in statements {
        println!("{}", stage);
        for sql in stage_statements {
            execute_statement(backend, sql).await?;
        }
        println!();
    }

    println!("🎉 Demo complete! Current state:");
    show_status(backend).await?;
    
    println!("\n🧪 Testing permission checks:");
    
    let test_checks = vec![
        ("ROLE data_scientist", "sales.orders", "SELECT"),
        ("ROLE data_scientist", "sales.orders", "DELETE"), 
        ("ROLE analyst", "sales.customers", "SELECT"),
        ("ROLE intern", "sales.orders", "INSERT"),
    ];
    
    for (principal, resource, action) in test_checks {
        check_permission(backend, principal, resource, action).await?;
    }

    Ok(())
}

async fn run_row_level_security_demo(backend: &mut EmulatorBackend) -> Result<()> {
    println!("🔐 Row-Level Security Demo 🔐\n");
    println!("Testing advanced Lake Formation row-level filtering...\n");

    // Set up base permissions with row-level filters
    let statements = vec![
        "CREATE ROLE regional_manager",
        "CREATE ROLE department_head", 
        "CREATE ROLE employee",
    ];

    println!("📝 Creating roles for row-level security demo...");
    for sql in statements {
        execute_statement(backend, sql).await?;
    }

    // For now, we'll manually create permissions with row filters
    // In the future, the parser will handle this syntax
    println!("\n🔧 Setting up row-level permissions...");
    
    // Create permissions with row filters programmatically
    let regional_permission = Permission {
        principal: Principal::Role("regional_manager".to_string()),
        resource: Resource::Table {
            database: "sales".to_string(),
            table: "orders".to_string(),
            columns: None,
        },
        actions: vec![Action::Select],
        grant_option: false,
        row_filter: Some(RowFilter {
            expression: "region = SESSION_CONTEXT('user_region')".to_string(),
            session_context: None,
        }),
    };

    let department_permission = Permission {
        principal: Principal::Role("department_head".to_string()),
        resource: Resource::Table {
            database: "hr".to_string(),
            table: "employees".to_string(),
            columns: None,
        },
        actions: vec![Action::Select],
        grant_option: false,
        row_filter: Some(RowFilter {
            expression: "department = SESSION_CONTEXT('user_department') AND region = SESSION_CONTEXT('user_region')".to_string(),
            session_context: None,
        }),
    };

    // Grant permissions directly
    backend.grant_permissions(regional_permission).await?;
    backend.grant_permissions(department_permission).await?;

    println!("✅ Set up row-level permissions:");
    println!("   • regional_manager can see orders WHERE region = SESSION_CONTEXT('user_region')");
    println!("   • department_head can see employees WHERE department = SESSION_CONTEXT('user_department') AND region = SESSION_CONTEXT('user_region')");
    
    println!("\n🧪 Testing row-level security scenarios:\n");

    // Test scenarios with different session contexts
    let scenarios = vec![
        (
            "West Coast Regional Manager",
            create_session_context(vec![("user_region", "west")]),
            vec![
                (Principal::Role("regional_manager".to_string()), "sales.orders", Action::Select),
            ]
        ),
        (
            "East Coast Regional Manager", 
            create_session_context(vec![("user_region", "east")]),
            vec![
                (Principal::Role("regional_manager".to_string()), "sales.orders", Action::Select),
            ]
        ),
        (
            "Engineering Department Head (West)",
            create_session_context(vec![
                ("user_department", "engineering"),
                ("user_region", "west")
            ]),
            vec![
                (Principal::Role("department_head".to_string()), "hr.employees", Action::Select),
            ]
        ),
        (
            "Finance Department Head (East)",
            create_session_context(vec![
                ("user_department", "finance"), 
                ("user_region", "east")
            ]),
            vec![
                (Principal::Role("department_head".to_string()), "hr.employees", Action::Select),
            ]
        ),
    ];

    for (scenario_name, session_context, tests) in scenarios {
        println!("👤 **{}:**", scenario_name);
        println!("   Session Context: {:?}", session_context);
        
        for (principal, resource_str, action) in tests {
            let resource = parse_resource(resource_str)?;
            let allowed = backend.test_row_level_security(&principal, &resource, &action, session_context.clone()).await?;
            
            println!("   🔍 {} → {:?} → {}: {}", 
                format!("{:?}", principal).replace("Role(\"", "").replace("\")", ""),
                action,
                resource_str,
                if allowed { "✅ ALLOWED" } else { "❌ DENIED" }
            );
        }
        println!();
    }

    println!("🎯 **Key Insights:**");
    println!("   • Each user only sees data from THEIR region/department");
    println!("   • Same role, different session context = different access");
    println!("   • Row-level security enforced automatically!");

    Ok(())
}

async fn check_permission(backend: &EmulatorBackend, principal_str: &str, resource_str: &str, action_str: &str) -> Result<()> {
    // Parse principal
    let principal = parse_principal(principal_str)?;
    
    // Parse resource  
    let resource = parse_resource(resource_str)?;
    
    // Parse action
    let action = parse_action(action_str)?;

    let allowed = backend.check_permissions(&principal, &resource, &action).await?;
    
    println!("🔍 {} → {} → {}: {}", 
        principal_str, 
        action_str,
        resource_str, 
        if allowed { "✅ ALLOWED" } else { "❌ DENIED" }
    );
    
    Ok(())
}

async fn show_status(backend: &EmulatorBackend) -> Result<()> {
    let state = backend.get_state();
    
    println!("📊 **Lake Formation Emulator Status**");
    println!("====================================");
    println!("• Permissions: {}", state.permissions.len());
    println!("• Roles: {}", state.roles.len());
    println!("• Tags: {}", state.tags.len());
    println!("• Session Context: {}", state.session_context.len());
    
    if !state.roles.is_empty() {
        println!("\n👥 **Roles:**");
        for (role_name, members) in &state.roles {
            println!("  • {}: {} member(s)", role_name, members.len());
        }
    }
    
    if !state.tags.is_empty() {
        println!("\n🏷️ **Tags:**");
        for tag in state.tags.values() {
            println!("  • {}: {:?}", tag.key, tag.values);
        }
    }
    
    if !state.permissions.is_empty() {
        println!("\n🔐 **Permissions:**");
        for (i, permission) in state.permissions.iter().enumerate() {
            let filter_info = if permission.row_filter.is_some() { " [ROW-LEVEL]" } else { "" };
            println!("  {}. {:?} → {:?} → {:?}{}", 
                i + 1, permission.principal, permission.actions, permission.resource, filter_info);
        }
    }
    
    Ok(())
}

async fn export_state(backend: &EmulatorBackend, format: &str) -> Result<()> {
    let state = backend.get_state();
    
    match format {
        "sql" => {
            let sql = lakesql_emulator::storage::StateExporter::to_sql_ddl(state);
            println!("{}", sql);
        },
        "summary" | _ => {
            let summary = lakesql_emulator::storage::StateExporter::to_summary(state);
            println!("{}", summary);
        },
    }
    
    Ok(())
}

// Helper parsing functions
fn parse_principal(s: &str) -> Result<Principal> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    match parts.get(0) {
        Some(&"ROLE") => Ok(Principal::Role(parts[1].to_string())),
        Some(&"USER") => Ok(Principal::User(parts[1].trim_matches('\'').to_string())),
        Some(&"GROUP") => Ok(Principal::SamlGroup(parts[1].trim_matches('\'').to_string())),
        _ => Err(anyhow::anyhow!("Invalid principal format: {}", s)),
    }
}

fn parse_resource(s: &str) -> Result<Resource> {
    if s.starts_with("DATABASE ") {
        Ok(Resource::Database {
            name: s.strip_prefix("DATABASE ").unwrap().to_string(),
        })
    } else if s.contains('.') {
        let parts: Vec<&str> = s.split('.').collect();
        Ok(Resource::Table {
            database: parts[0].to_string(),
            table: parts[1].to_string(),
            columns: None,
        })
    } else {
        Err(anyhow::anyhow!("Invalid resource format: {}", s))
    }
}

fn parse_action(s: &str) -> Result<Action> {
    match s.to_uppercase().as_str() {
        "SELECT" => Ok(Action::Select),
        "INSERT" => Ok(Action::Insert),
        "UPDATE" => Ok(Action::Update),
        "DELETE" => Ok(Action::Delete),
        "CREATE_TABLE" => Ok(Action::CreateTable),
        "DROP_TABLE" => Ok(Action::DropTable),
        "ALTER_TABLE" => Ok(Action::AlterTable),
        "DESCRIBE" => Ok(Action::Describe),
        _ => Err(anyhow::anyhow!("Invalid action: {}", s)),
    }
}

fn create_session_context(data: Vec<(&str, &str)>) -> HashMap<String, String> {
    data.into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}