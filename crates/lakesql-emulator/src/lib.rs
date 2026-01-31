//! # Lake Formation Emulator
//! 
//! In-memory implementation of Lake Formation DDL operations.
//! Perfect for local development and testing.

use lakesql_core::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use anyhow::Result;
use async_trait::async_trait;

pub mod storage;
pub mod engine;
pub mod expression;

pub use engine::EmulatorEngine;

/// Complete state of the Lake Formation emulator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmulatorState {
    /// All granted permissions
    pub permissions: Vec<Permission>,
    /// All defined roles (role_name -> members)
    pub roles: HashMap<String, HashSet<String>>,
    /// All defined LF-Tags (tag_key -> allowed_values)
    pub tags: HashMap<String, LfTag>,
    /// Session context for row-level security
    pub session_context: HashMap<String, String>,
}

impl EmulatorState {
    pub fn new() -> Self {
        Self {
            permissions: Vec::new(),
            roles: HashMap::new(),
            tags: HashMap::new(),
            session_context: HashMap::new(),
        }
    }
}

impl Default for EmulatorState {
    fn default() -> Self {
        Self::new()
    }
}

/// Lake Formation Emulator Backend
pub struct EmulatorBackend {
    /// Current state
    state: EmulatorState,
    /// Optional file path for persistence  
    state_file: Option<String>,
    /// Permission evaluation engine
    engine: EmulatorEngine,
}

impl EmulatorBackend {
    /// Create a new emulator backend
    pub async fn new(state_file: Option<String>) -> Result<Self> {
        let mut backend = Self {
            state: EmulatorState::new(),
            state_file: state_file.clone(),
            engine: EmulatorEngine::new(),
        };

        // Load existing state if file exists
        if let Some(ref file_path) = state_file {
            if Path::new(file_path).exists() {
                backend.load_state(file_path).await?;
            }
        }

        Ok(backend)
    }

    /// Load state from file
    async fn load_state(&mut self, file_path: &str) -> Result<()> {
        let content = tokio::fs::read_to_string(file_path).await?;
        self.state = serde_json::from_str(&content)?;
        self.engine.update_state(&self.state);
        println!("📂 Loaded emulator state from: {}", file_path);
        Ok(())
    }

    /// Save state to file
    async fn save_state(&self) -> Result<()> {
        if let Some(ref file_path) = self.state_file {
            let content = serde_json::to_string_pretty(&self.state)?;
            tokio::fs::write(file_path, content).await?;
            println!("💾 Saved emulator state to: {}", file_path);
        }
        Ok(())
    }

    /// Execute a DDL statement by parsing and applying it
    pub async fn execute_ddl_direct(&mut self, statement: lakesql_parser::DdlStatement) -> Result<DdlResult> {
        use lakesql_parser::DdlStatement;

        match statement {
            DdlStatement::Grant { actions, resource, principal, grant_option, row_filter } => {
                let permission = Permission {
                    principal,
                    resource,
                    actions,
                    grant_option,
                    row_filter,
                };
                self.grant_permissions(permission).await
            },
            
            DdlStatement::Revoke { actions, resource, principal } => {
                self.revoke_permissions(&principal, &resource, &actions).await
            },
            
            DdlStatement::CreateRole { name } => {
                self.state.roles.insert(name.clone(), HashSet::new());
                self.engine.update_state(&self.state);
                self.save_state().await?;
                Ok(DdlResult::Success { 
                    message: format!("Created role: {}", name) 
                })
            },
            
            DdlStatement::CreateTag { name, values } => {
                let tag = LfTag {
                    key: name.clone(),
                    values,
                    description: None,
                };
                self.create_tag(tag).await
            },
            
            DdlStatement::DropRole { name } => {
                self.state.roles.remove(&name);
                // Remove all permissions for this role
                self.state.permissions.retain(|p| {
                    !matches!(p.principal, Principal::Role(ref role_name) if role_name == &name)
                });
                self.engine.update_state(&self.state);
                self.save_state().await?;
                Ok(DdlResult::Success { 
                    message: format!("Dropped role: {}", name) 
                })
            },
            
            DdlStatement::DropTag { name } => {
                self.delete_tag(&name).await
            },
            
            DdlStatement::ShowPermissions { principal } => {
                let permissions = if let Some(p) = principal {
                    self.list_permissions_for_principal(&p).await?
                } else {
                    self.state.permissions.clone()
                };
                
                let message = format!("Found {} permissions", permissions.len());
                Ok(DdlResult::Success { message })
            },
            
            DdlStatement::ShowRoles => {
                let roles: Vec<_> = self.state.roles.keys().collect();
                let message = format!("Roles: {:?}", roles);
                Ok(DdlResult::Success { message })
            },
            
            DdlStatement::ShowTags => {
                let tags: Vec<_> = self.state.tags.keys().collect();
                let message = format!("Tags: {:?}", tags);
                Ok(DdlResult::Success { message })
            },
        }
    }

    /// Get current state (for debugging/inspection)
    pub fn get_state(&self) -> &EmulatorState {
        &self.state
    }

    /// Test row-level security with custom session context
    pub async fn test_row_level_security(
        &mut self,
        principal: &Principal,
        resource: &Resource,
        action: &Action,
        session_context: HashMap<String, String>
    ) -> Result<bool> {
        // Set session context
        self.state.session_context = session_context;
        self.engine.update_state(&self.state);
        
        // Check permission with row-level filters
        self.check_permissions(principal, resource, action).await
    }
}

#[async_trait]
impl LakeFormationBackend for EmulatorBackend {
    async fn execute_ddl(&mut self, sql: &str) -> Result<DdlResult> {
        use lakesql_parser::parse_ddl;
        
        // Parse the DDL statement
        let statement = parse_ddl(sql)?;
        
        // Execute it directly
        self.execute_ddl_direct(statement).await
    }

    async fn grant_permissions(&mut self, permission: Permission) -> Result<DdlResult> {
        // Remove any existing permission for same principal/resource combination
        self.state.permissions.retain(|p| {
            !(p.principal == permission.principal && p.resource == permission.resource)
        });

        // Add the new permission
        let message = format!(
            "Granted {:?} on {:?} to {:?}", 
            permission.actions, permission.resource, permission.principal
        );
        
        self.state.permissions.push(permission);
        self.engine.update_state(&self.state);
        self.save_state().await?;
        
        Ok(DdlResult::Success { message })
    }

    async fn revoke_permissions(
        &mut self, 
        principal: &Principal, 
        resource: &Resource, 
        actions: &[Action]
    ) -> Result<DdlResult> {
        let initial_count = self.state.permissions.len();

        // Remove permissions that match principal, resource, and any of the actions
        self.state.permissions.retain(|p| {
            !(p.principal == *principal && 
              p.resource == *resource &&
              actions.iter().any(|a| p.actions.contains(a)))
        });

        let removed_count = initial_count - self.state.permissions.len();
        self.engine.update_state(&self.state);
        self.save_state().await?;

        let message = format!(
            "Revoked {} permission(s) for {:?} on {:?}", 
            removed_count, principal, resource
        );
        
        Ok(DdlResult::Success { message })
    }

    async fn check_permissions(
        &self, 
        principal: &Principal, 
        resource: &Resource, 
        action: &Action
    ) -> Result<bool> {
        let allowed = self.engine.check_permission(principal, resource, action);
        Ok(allowed)
    }

    async fn create_tag(&mut self, tag: LfTag) -> Result<DdlResult> {
        let message = format!("Created tag: {} with values {:?}", tag.key, tag.values);
        self.state.tags.insert(tag.key.clone(), tag);
        self.engine.update_state(&self.state);
        self.save_state().await?;
        Ok(DdlResult::Success { message })
    }

    async fn delete_tag(&mut self, tag_key: &str) -> Result<DdlResult> {
        self.state.tags.remove(tag_key);
        // TODO: Remove any tag-based permissions
        self.engine.update_state(&self.state);
        self.save_state().await?;
        Ok(DdlResult::Success { 
            message: format!("Deleted tag: {}", tag_key) 
        })
    }

    async fn list_permissions_for_principal(&self, principal: &Principal) -> Result<Vec<Permission>> {
        let permissions = self.state.permissions
            .iter()
            .filter(|p| p.principal.matches(principal))
            .cloned()
            .collect();
        Ok(permissions)
    }

    async fn list_permissions_for_resource(&self, resource: &Resource) -> Result<Vec<Permission>> {
        let permissions = self.state.permissions
            .iter()
            .filter(|p| resource.is_covered_by(&p.resource))
            .cloned()
            .collect();
        Ok(permissions)
    }

    async fn set_session_context(&mut self, context: HashMap<String, String>) -> Result<()> {
        self.state.session_context = context;
        self.engine.update_state(&self.state);
        self.save_state().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_operations() {
        let mut backend = EmulatorBackend::new(None).await.unwrap();

        // Test DDL execution
        let result = backend.execute_ddl("CREATE ROLE data_scientist").await.unwrap();
        match result {
            DdlResult::Success { message } => {
                assert!(message.contains("Created role: data_scientist"));
            },
            _ => panic!("Expected success"),
        }

        // Check that role was created
        assert!(backend.state.roles.contains_key("data_scientist"));

        // Test permission grant
        let result = backend.execute_ddl(
            "GRANT SELECT ON sales.orders TO ROLE data_scientist"
        ).await.unwrap();
        
        match result {
            DdlResult::Success { .. } => {
                assert_eq!(backend.state.permissions.len(), 1);
            },
            _ => panic!("Expected success"),
        }
    }

    #[tokio::test]
    async fn test_permission_checking() {
        let mut backend = EmulatorBackend::new(None).await.unwrap();

        // Create role and grant permission
        backend.execute_ddl("CREATE ROLE analyst").await.unwrap();
        backend.execute_ddl("GRANT SELECT ON sales.orders TO ROLE analyst").await.unwrap();

        // Check permission
        let principal = Principal::Role("analyst".to_string());
        let resource = Resource::Table {
            database: "sales".to_string(),
            table: "orders".to_string(),
            columns: None,
        };
        let action = Action::Select;

        let allowed = backend.check_permissions(&principal, &resource, &action).await.unwrap();
        assert!(allowed);

        // Check denied permission
        let denied = backend.check_permissions(&principal, &resource, &Action::Delete).await.unwrap();
        assert!(!denied);
    }
}