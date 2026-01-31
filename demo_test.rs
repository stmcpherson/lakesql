use lakesql_parser::parse_ddl;

fn main() {
    println!("🦀 Lake Formation DDL Demo 🦀\n");
    
    let statements = [
        "GRANT SELECT ON sales.orders TO ROLE data_scientist",
        "CREATE ROLE analytics_team", 
        "CREATE TAG department VALUES ('finance', 'marketing', 'engineering')",
    ];
    
    for sql in statements {
        println!("📝 Parsing: {}", sql);
        match parse_ddl(sql) {
            Ok(statement) => println!("✅ Success: {:#?}\n", statement),
            Err(e) => println!("❌ Error: {}\n", e),
        }
    }
}
