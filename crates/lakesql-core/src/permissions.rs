//! Permission management and evaluation logic

use crate::types::*;
use std::collections::{HashMap, HashSet};
use anyhow::Result;

/// Permission evaluation engine
#[derive(Debug, Clone)]
pub struct PermissionEngine {
    /// All granted permissions
    permissions: Vec<Permission>,
    /// Defined LF-Tags
    tags: HashMap<String, LfTag>,
    /// Session context for row-level security
    session_context: HashMap<String, String>,
}

impl PermissionEngine {
    pub fn new() -> Self {
        Self {
            permissions: Vec::new(),
            tags: HashMap::new(),
            session_context: HashMap::new(),
        }
    }

    /// Grant a permission
    pub fn grant_permission(&mut self, permission: Permission) -> Result<()> {
        // Remove any existing conflicting permissions for same principal/resource
        self.permissions.retain(|p| {
            !(p.principal == permission.principal && p.resource == permission.resource)
        });
        
        self.permissions.push(permission);
        Ok(())
    }

    /// Revoke a permission  
    pub fn revoke_permission(&mut self, principal: &Principal, resource: &Resource, actions: &[Action]) -> Result<()> {
        self.permissions.retain(|p| {
            !(p.principal == *principal && 
              p.resource == *resource &&
              actions.iter().any(|a| p.actions.contains(a)))
        });
        Ok(())
    }

    /// Check if a principal has specific permissions on a resource
    pub fn check_permission(&self, principal: &Principal, resource: &Resource, action: &Action) -> bool {
        for permission in &self.permissions {
            if permission.principal.matches(principal) &&
               permission.actions.contains(action) &&
               resource.is_covered_by(&permission.resource) {
                
                // Check row-level filters if present
                if let Some(row_filter) = &permission.row_filter {
                    if !self.evaluate_row_filter(row_filter, resource) {
                        continue;
                    }
                }
                
                return true;
            }
        }
        false
    }

    /// Evaluate row-level security filters
    fn evaluate_row_filter(&self, filter: &RowFilter, _resource: &Resource) -> bool {
        // TODO: Implement actual expression evaluation
        // For now, just check if session context matches
        if let Some(ref context) = filter.session_context {
            for (key, value) in context {
                if let Some(session_value) = self.session_context.get(key) {
                    if session_value != value {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Set session context for row-level security
    pub fn set_session_context(&mut self, key: String, value: String) {
        self.session_context.insert(key, value);
    }

    /// Create or update an LF-Tag
    pub fn create_tag(&mut self, tag: LfTag) -> Result<()> {
        self.tags.insert(tag.key.clone(), tag);
        Ok(())
    }

    /// Get all permissions for a principal
    pub fn get_permissions_for_principal(&self, principal: &Principal) -> Vec<&Permission> {
        self.permissions
            .iter()
            .filter(|p| p.principal.matches(principal))
            .collect()
    }

    /// Get all permissions for a resource
    pub fn get_permissions_for_resource(&self, resource: &Resource) -> Vec<&Permission> {
        self.permissions
            .iter()
            .filter(|p| resource.is_covered_by(&p.resource))
            .collect()
    }

    /// List all unique principals
    pub fn list_principals(&self) -> HashSet<&Principal> {
        self.permissions.iter().map(|p| &p.principal).collect()
    }

    /// List all unique resources  
    pub fn list_resources(&self) -> HashSet<&Resource> {
        self.permissions.iter().map(|p| &p.resource).collect()
    }
}

impl Default for PermissionEngine {
    fn default() -> Self {
        Self::new()
    }
}