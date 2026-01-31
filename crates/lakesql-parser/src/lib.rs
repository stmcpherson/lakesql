//! Lake Formation DDL Parser
//! 
//! Parses Lake Formation DDL statements into AST structures

use pest::Parser;
use pest_derive::Parser;
use anyhow::{Result, anyhow};
use lakesql_core::types::*;

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct LakeSqlParser;

/// Abstract Syntax Tree for Lake Formation DDL
#[derive(Debug, Clone, PartialEq)]
pub enum DdlStatement {
    Grant {
        actions: Vec<Action>,
        resource: Resource,
        principal: Principal,
        grant_option: bool,
        row_filter: Option<RowFilter>,
    },
    Revoke {
        actions: Vec<Action>,
        resource: Resource,
        principal: Principal,
    },
    CreateRole {
        name: String,
    },
    CreateTag {
        name: String,
        values: Vec<String>,
    },
    DropRole {
        name: String,
    },
    DropTag {
        name: String,
    },
    ShowPermissions {
        principal: Option<Principal>,
    },
    ShowRoles,
    ShowTags,
}

impl DdlStatement {
    /// Convert DDL statement to Permission (for GRANT/REVOKE)
    pub fn to_permission(&self) -> Result<Permission> {
        match self {
            DdlStatement::Grant { actions, resource, principal, grant_option, row_filter } => {
                Ok(Permission {
                    principal: principal.clone(),
                    resource: resource.clone(),
                    actions: actions.clone(),
                    grant_option: *grant_option,
                    row_filter: row_filter.clone(),
                })
            },
            _ => Err(anyhow!("Statement is not a GRANT and cannot be converted to Permission")),
        }
    }
}

/// Parse a Lake Formation DDL statement
pub fn parse_ddl(sql: &str) -> Result<DdlStatement> {
    let pairs = LakeSqlParser::parse(Rule::program, sql)
        .map_err(|e| anyhow!("Parse error: {}", e))?;

    for pair in pairs {
        match pair.as_rule() {
            Rule::program => {
                for inner_pair in pair.into_inner() {
                    if inner_pair.as_rule() == Rule::ddl_statement {
                        return parse_ddl_statement(inner_pair);
                    }
                }
            },
            _ => continue,
        }
    }

    Err(anyhow!("No valid DDL statement found"))
}

fn parse_ddl_statement(pair: pest::iterators::Pair<Rule>) -> Result<DdlStatement> {
    for inner_pair in pair.into_inner() {
        return match inner_pair.as_rule() {
            Rule::grant_statement => parse_grant_statement(inner_pair),
            Rule::revoke_statement => parse_revoke_statement(inner_pair),
            Rule::create_role_statement => parse_create_role_statement(inner_pair),
            Rule::create_tag_statement => parse_create_tag_statement(inner_pair),
            Rule::drop_role_statement => parse_drop_role_statement(inner_pair),
            Rule::drop_tag_statement => parse_drop_tag_statement(inner_pair),
            Rule::show_statement => parse_show_statement(inner_pair),
            _ => Err(anyhow!("Unknown DDL statement type")),
        };
    }
    
    Err(anyhow!("Empty DDL statement"))
}

fn parse_grant_statement(pair: pest::iterators::Pair<Rule>) -> Result<DdlStatement> {
    let mut actions = Vec::new();
    let mut resource = None;
    let mut principal = None;
    let mut grant_option = false;
    let mut row_filter = None;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::action_list => {
                actions = parse_action_list(inner_pair)?;
            },
            Rule::resource => {
                resource = Some(parse_resource(inner_pair)?);
            },
            Rule::principal => {
                principal = Some(parse_principal(inner_pair)?);
            },
            Rule::grant => {
                // Look for "WITH GRANT OPTION"
                grant_option = true;
            },
            Rule::row_filter => {
                row_filter = Some(parse_row_filter(inner_pair)?);
            },
            _ => {},
        }
    }

    Ok(DdlStatement::Grant {
        actions,
        resource: resource.ok_or_else(|| anyhow!("Missing resource in GRANT"))?,
        principal: principal.ok_or_else(|| anyhow!("Missing principal in GRANT"))?,
        grant_option,
        row_filter,
    })
}

fn parse_revoke_statement(pair: pest::iterators::Pair<Rule>) -> Result<DdlStatement> {
    let mut actions = Vec::new();
    let mut resource = None;
    let mut principal = None;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::action_list => {
                actions = parse_action_list(inner_pair)?;
            },
            Rule::resource => {
                resource = Some(parse_resource(inner_pair)?);
            },
            Rule::principal => {
                principal = Some(parse_principal(inner_pair)?);
            },
            _ => {},
        }
    }

    Ok(DdlStatement::Revoke {
        actions,
        resource: resource.ok_or_else(|| anyhow!("Missing resource in REVOKE"))?,
        principal: principal.ok_or_else(|| anyhow!("Missing principal in REVOKE"))?,
    })
}

fn parse_create_role_statement(pair: pest::iterators::Pair<Rule>) -> Result<DdlStatement> {
    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::identifier {
            return Ok(DdlStatement::CreateRole {
                name: inner_pair.as_str().to_string(),
            });
        }
    }
    Err(anyhow!("Missing role name in CREATE ROLE"))
}

fn parse_create_tag_statement(pair: pest::iterators::Pair<Rule>) -> Result<DdlStatement> {
    let mut name = None;
    let mut values = Vec::new();

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::identifier => {
                name = Some(inner_pair.as_str().to_string());
            },
            Rule::string_list => {
                values = parse_string_list(inner_pair)?;
            },
            _ => {},
        }
    }

    Ok(DdlStatement::CreateTag {
        name: name.ok_or_else(|| anyhow!("Missing tag name"))?,
        values,
    })
}

fn parse_drop_role_statement(pair: pest::iterators::Pair<Rule>) -> Result<DdlStatement> {
    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::identifier {
            return Ok(DdlStatement::DropRole {
                name: inner_pair.as_str().to_string(),
            });
        }
    }
    Err(anyhow!("Missing role name in DROP ROLE"))
}

fn parse_drop_tag_statement(pair: pest::iterators::Pair<Rule>) -> Result<DdlStatement> {
    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::identifier {
            return Ok(DdlStatement::DropTag {
                name: inner_pair.as_str().to_string(),
            });
        }
    }
    Err(anyhow!("Missing tag name in DROP TAG"))
}

fn parse_show_statement(pair: pest::iterators::Pair<Rule>) -> Result<DdlStatement> {
    for inner_pair in pair.into_inner() {
        return match inner_pair.as_rule() {
            Rule::show_permissions_statement => {
                // TODO: Parse optional principal
                Ok(DdlStatement::ShowPermissions { principal: None })
            },
            Rule::show_roles_statement => Ok(DdlStatement::ShowRoles),
            Rule::show_tags_statement => Ok(DdlStatement::ShowTags),
            _ => Err(anyhow!("Unknown SHOW statement type")),
        };
    }
    Err(anyhow!("Empty SHOW statement"))
}

// Helper parsing functions
fn parse_action_list(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Action>> {
    let mut actions = Vec::new();
    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::action {
            actions.push(parse_action(inner_pair)?);
        }
    }
    Ok(actions)
}

fn parse_action(pair: pest::iterators::Pair<Rule>) -> Result<Action> {
    match pair.as_str().to_uppercase().as_str() {
        "SELECT" => Ok(Action::Select),
        "INSERT" => Ok(Action::Insert), 
        "UPDATE" => Ok(Action::Update),
        "DELETE" => Ok(Action::Delete),
        "CREATE_TABLE" => Ok(Action::CreateTable),
        "DROP_TABLE" => Ok(Action::DropTable),
        "ALTER_TABLE" => Ok(Action::AlterTable),
        "DESCRIBE" => Ok(Action::Describe),
        "DATA_LOCATION_ACCESS" => Ok(Action::DataLocationAccess),
        _ => Err(anyhow!("Unknown action: {}", pair.as_str())),
    }
}

fn parse_principal(pair: pest::iterators::Pair<Rule>) -> Result<Principal> {
    for inner_pair in pair.into_inner() {
        return match inner_pair.as_rule() {
            Rule::role_principal => {
                for p in inner_pair.into_inner() {
                    if p.as_rule() == Rule::identifier {
                        return Ok(Principal::Role(p.as_str().to_string()));
                    }
                }
                Err(anyhow!("Missing role name"))
            },
            Rule::user_principal => {
                for p in inner_pair.into_inner() {
                    if p.as_rule() == Rule::string_literal {
                        let user = p.as_str().trim_matches('\'').to_string();
                        return Ok(Principal::User(user));
                    }
                }
                Err(anyhow!("Missing user name"))
            },
            Rule::group_principal => {
                for p in inner_pair.into_inner() {
                    if p.as_rule() == Rule::string_literal {
                        let group = p.as_str().trim_matches('\'').to_string();
                        return Ok(Principal::SamlGroup(group));
                    }
                }
                Err(anyhow!("Missing group name"))
            },
            Rule::external_account_principal => {
                for p in inner_pair.into_inner() {
                    if p.as_rule() == Rule::string_literal {
                        let account = p.as_str().trim_matches('\'').to_string();
                        return Ok(Principal::ExternalAccount(account));
                    }
                }
                Err(anyhow!("Missing external account"))
            },
            _ => Err(anyhow!("Unknown principal type")),
        };
    }
    Err(anyhow!("Empty principal"))
}

fn parse_resource(pair: pest::iterators::Pair<Rule>) -> Result<Resource> {
    for inner_pair in pair.into_inner() {
        return match inner_pair.as_rule() {
            Rule::database_resource => {
                for p in inner_pair.into_inner() {
                    if p.as_rule() == Rule::identifier {
                        return Ok(Resource::Database {
                            name: p.as_str().to_string(),
                        });
                    }
                }
                Err(anyhow!("Missing database name"))
            },
            Rule::table_resource => parse_table_resource(inner_pair),
            Rule::data_location_resource => {
                let path = inner_pair.as_str().trim_matches('\'').to_string();
                Ok(Resource::DataLocation { path })
            },
            _ => Err(anyhow!("Unknown resource type")),
        };
    }
    Err(anyhow!("Empty resource"))
}

fn parse_table_resource(pair: pest::iterators::Pair<Rule>) -> Result<Resource> {
    let mut database = None;
    let mut table = None;
    let mut columns = None;

    let inner_pairs: Vec<_> = pair.into_inner().collect();
    
    if inner_pairs.len() >= 2 {
        database = Some(inner_pairs[0].as_str().to_string());
        table = Some(inner_pairs[1].as_str().to_string());
        
        if inner_pairs.len() > 2 && inner_pairs[2].as_rule() == Rule::column_list {
            columns = Some(parse_column_list(inner_pairs[2].clone())?);
        }
    }

    Ok(Resource::Table {
        database: database.ok_or_else(|| anyhow!("Missing database name"))?,
        table: table.ok_or_else(|| anyhow!("Missing table name"))?,
        columns,
    })
}

fn parse_column_list(pair: pest::iterators::Pair<Rule>) -> Result<Vec<String>> {
    let mut columns = Vec::new();
    for inner_pair in pair.into_inner() {
        if matches!(inner_pair.as_rule(), Rule::column_name) {
            columns.push(inner_pair.as_str().trim_matches('"').to_string());
        }
    }
    Ok(columns)
}

fn parse_row_filter(pair: pest::iterators::Pair<Rule>) -> Result<RowFilter> {
    // For now, just capture the raw expression
    // TODO: Implement proper expression parsing
    Ok(RowFilter {
        expression: pair.as_str().to_string(),
        session_context: None,
    })
}

fn parse_string_list(pair: pest::iterators::Pair<Rule>) -> Result<Vec<String>> {
    let mut strings = Vec::new();
    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::string_literal {
            strings.push(inner_pair.as_str().trim_matches('\'').to_string());
        }
    }
    Ok(strings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_grant() {
        let sql = "GRANT SELECT ON sales.orders TO ROLE data_scientist";
        let result = parse_ddl(sql).unwrap();
        
        match result {
            DdlStatement::Grant { actions, resource, principal, .. } => {
                assert_eq!(actions.len(), 1);
                assert_eq!(actions[0], Action::Select);
                assert_eq!(principal, Principal::Role("data_scientist".to_string()));
                match resource {
                    Resource::Table { database, table, .. } => {
                        assert_eq!(database, "sales");
                        assert_eq!(table, "orders");
                    },
                    _ => panic!("Expected table resource"),
                }
            },
            _ => panic!("Expected Grant statement"),
        }
    }

    #[test]
    fn test_create_role() {
        let sql = "CREATE ROLE analytics_team";
        let result = parse_ddl(sql).unwrap();
        
        match result {
            DdlStatement::CreateRole { name } => {
                assert_eq!(name, "analytics_team");
            },
            _ => panic!("Expected CreateRole statement"),
        }
    }

    #[test]
    fn test_create_tag() {
        let sql = "CREATE TAG department VALUES ('finance', 'marketing', 'engineering')";
        let result = parse_ddl(sql).unwrap();
        
        match result {
            DdlStatement::CreateTag { name, values } => {
                assert_eq!(name, "department");
                assert_eq!(values, vec!["finance", "marketing", "engineering"]);
            },
            _ => panic!("Expected CreateTag statement"),
        }
    }
}