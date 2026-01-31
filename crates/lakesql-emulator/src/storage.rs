//! Persistent storage for the Lake Formation emulator

use crate::EmulatorState;
use anyhow::Result;
// serde traits already available through EmulatorState
use std::path::Path;

/// Storage backend for emulator state
#[derive(Debug)]
pub struct FileStorage {
    file_path: String,
}

impl FileStorage {
    pub fn new(file_path: String) -> Self {
        Self { file_path }
    }

    /// Load state from file
    pub async fn load(&self) -> Result<EmulatorState> {
        if !Path::new(&self.file_path).exists() {
            return Ok(EmulatorState::new());
        }

        let content = tokio::fs::read_to_string(&self.file_path).await?;
        let state: EmulatorState = serde_json::from_str(&content)?;
        Ok(state)
    }

    /// Save state to file
    pub async fn save(&self, state: &EmulatorState) -> Result<()> {
        let content = serde_json::to_string_pretty(state)?;
        
        // Create parent directory if it doesn't exist
        if let Some(parent) = Path::new(&self.file_path).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        tokio::fs::write(&self.file_path, content).await?;
        Ok(())
    }

    /// Check if storage file exists
    pub fn exists(&self) -> bool {
        Path::new(&self.file_path).exists()
    }
}

/// Export state to different formats
pub struct StateExporter;

impl StateExporter {
    /// Export state as SQL DDL statements
    pub fn to_sql_ddl(state: &EmulatorState) -> String {
        let mut sql = String::new();
        sql.push_str("-- Lake Formation Emulator State Export\n");
        sql.push_str("-- Generated DDL statements to recreate this state\n\n");

        // Export roles
        for role_name in state.roles.keys() {
            sql.push_str(&format!("CREATE ROLE {};\n", role_name));
        }
        sql.push_str("\n");

        // Export tags
        for tag in state.tags.values() {
            let values_str = tag.values
                .iter()
                .map(|v| format!("'{}'", v))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!("CREATE TAG {} VALUES ({});\n", tag.key, values_str));
        }
        sql.push_str("\n");

        // Export permissions as GRANT statements
        for permission in &state.permissions {
            let actions_str = permission.actions
                .iter()
                .map(|a| format!("{:?}", a).to_uppercase())
                .collect::<Vec<_>>()
                .join(", ");

            let principal_str = match &permission.principal {
                lakesql_core::Principal::Role(name) => format!("ROLE {}", name),
                lakesql_core::Principal::User(name) => format!("USER '{}'", name),
                lakesql_core::Principal::SamlGroup(name) => format!("GROUP '{}'", name),
                lakesql_core::Principal::ExternalAccount(account) => format!("EXTERNAL_ACCOUNT '{}'", account),
                lakesql_core::Principal::TaggedPrincipal { tag_key, tag_values } => {
                    format!("TAGGED {}='{}'", tag_key, tag_values.join(","))
                },
            };

            let resource_str = match &permission.resource {
                lakesql_core::Resource::Database { name } => format!("DATABASE {}", name),
                lakesql_core::Resource::Table { database, table, columns } => {
                    if let Some(cols) = columns {
                        let cols_str = cols.join(", ");
                        format!("{}.{}({})", database, table, cols_str)
                    } else {
                        format!("{}.{}", database, table)
                    }
                },
                lakesql_core::Resource::DataLocation { path } => format!("'{}'", path),
                lakesql_core::Resource::TaggedResource { tag_conditions } => {
                    let conditions_str = tag_conditions
                        .iter()
                        .map(|(k, vs)| format!("{}='{}'", k, vs.join(",")))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    format!("RESOURCES TAGGED {}", conditions_str)
                },
            };

            let grant_option_str = if permission.grant_option {
                " WITH GRANT OPTION"
            } else {
                ""
            };

            let row_filter_str = if let Some(filter) = &permission.row_filter {
                format!(" WHERE {}", filter.expression)
            } else {
                String::new()
            };

            sql.push_str(&format!(
                "GRANT {} ON {} TO {}{}{};\\n",
                actions_str, resource_str, principal_str, grant_option_str, row_filter_str
            ));
        }

        sql
    }

    /// Export state as a human-readable summary
    pub fn to_summary(state: &EmulatorState) -> String {
        let mut summary = String::new();
        summary.push_str("🦀 Lake Formation Emulator State Summary\n");
        summary.push_str("=========================================\n\n");

        summary.push_str(&format!("📊 **Statistics:**\n"));
        summary.push_str(&format!("- Permissions: {}\n", state.permissions.len()));
        summary.push_str(&format!("- Roles: {}\n", state.roles.len()));
        summary.push_str(&format!("- Tags: {}\n", state.tags.len()));
        summary.push_str(&format!("- Session Context Keys: {}\n\n", state.session_context.len()));

        if !state.roles.is_empty() {
            summary.push_str("👥 **Roles:**\n");
            for (role_name, members) in &state.roles {
                summary.push_str(&format!("- {}: {} member(s)\n", role_name, members.len()));
                for member in members {
                    summary.push_str(&format!("  • {}\n", member));
                }
            }
            summary.push_str("\n");
        }

        if !state.tags.is_empty() {
            summary.push_str("🏷️ **Tags:**\n");
            for tag in state.tags.values() {
                summary.push_str(&format!("- {}: {:?}\n", tag.key, tag.values));
            }
            summary.push_str("\n");
        }

        if !state.permissions.is_empty() {
            summary.push_str("🔐 **Permissions:**\n");
            for (i, permission) in state.permissions.iter().enumerate() {
                summary.push_str(&format!("{}. {:?} → {:?} → {:?}\n", 
                    i + 1, permission.principal, permission.actions, permission.resource));
            }
        }

        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_file_storage() {
        let temp_file = NamedTempFile::new().unwrap();
        let storage = FileStorage::new(temp_file.path().to_string_lossy().to_string());

        // Save state
        let mut state = EmulatorState::new();
        state.roles.insert("test_role".to_string(), std::collections::HashSet::new());
        
        storage.save(&state).await.unwrap();

        // Load state
        let loaded_state = storage.load().await.unwrap();
        assert!(loaded_state.roles.contains_key("test_role"));
    }

    #[test]
    fn test_sql_export() {
        let mut state = EmulatorState::new();
        state.roles.insert("analyst".to_string(), std::collections::HashSet::new());
        
        let sql = StateExporter::to_sql_ddl(&state);
        assert!(sql.contains("CREATE ROLE analyst"));
    }
}