//! Permission evaluation engine for the Lake Formation emulator

use lakesql_core::*;
use crate::{EmulatorState, expression::ExpressionEvaluator};
use std::collections::HashMap;

/// Engine that evaluates permissions based on current state
#[derive(Debug)]
pub struct EmulatorEngine {
    /// Cached state for fast lookups
    state: EmulatorState,
}

impl EmulatorEngine {
    pub fn new() -> Self {
        Self {
            state: EmulatorState::new(),
        }
    }

    /// Update the engine with new state
    pub fn update_state(&mut self, state: &EmulatorState) {
        self.state = state.clone();
    }

    /// Check if a principal has permission to perform an action on a resource
    pub fn check_permission(&self, principal: &Principal, resource: &Resource, action: &Action) -> bool {
        // Check direct permissions
        for permission in &self.state.permissions {
            if self.matches_permission(principal, resource, action, permission) {
                return true;
            }
        }

        false
    }

    /// Check if a permission matches the request
    fn matches_permission(
        &self, 
        principal: &Principal, 
        resource: &Resource, 
        action: &Action, 
        permission: &Permission
    ) -> bool {
        // Check if principal matches
        if !self.principal_matches(principal, &permission.principal) {
            return false;
        }

        // Check if action is allowed
        if !permission.actions.contains(action) {
            return false;
        }

        // Check if resource is covered
        if !resource.is_covered_by(&permission.resource) {
            return false;
        }

        // Check row-level filters if present
        if let Some(ref row_filter) = permission.row_filter {
            if !self.evaluate_row_filter(row_filter, resource) {
                return false;
            }
        }

        true
    }

    /// Check if a principal matches (including role membership, tags, etc.)
    fn principal_matches(&self, request_principal: &Principal, permission_principal: &Principal) -> bool {
        match (request_principal, permission_principal) {
            // Exact matches
            (Principal::User(u1), Principal::User(u2)) => u1 == u2,
            (Principal::Role(r1), Principal::Role(r2)) => r1 == r2,
            (Principal::SamlGroup(g1), Principal::SamlGroup(g2)) => g1 == g2,
            (Principal::ExternalAccount(a1), Principal::ExternalAccount(a2)) => a1 == a2,

            // User can match role if they're a member
            (Principal::User(user), Principal::Role(role)) => {
                if let Some(members) = self.state.roles.get(role) {
                    members.contains(user)
                } else {
                    false
                }
            },

            // TODO: Implement tag-based matching
            (Principal::TaggedPrincipal { .. }, _) => {
                // For now, tagged principals don't match
                false
            },
            (_, Principal::TaggedPrincipal { .. }) => {
                // For now, tagged principals don't match
                false
            },

            // Different types don't match
            _ => false,
        }
    }

    /// Evaluate row-level security filters
    fn evaluate_row_filter(&self, row_filter: &RowFilter, _resource: &Resource) -> bool {
        // Create expression evaluator
        let mut evaluator = ExpressionEvaluator::new();
        
        // Set session context
        evaluator.set_session_context(self.state.session_context.clone());
        
        // For demo purposes, create some sample row data
        // In a real implementation, this would come from the actual data being queried
        let sample_row = self.create_sample_row_data(_resource);
        evaluator.set_row_data(sample_row);
        
        // Evaluate the filter
        match evaluator.evaluate_filter(row_filter) {
            Ok(result) => result,
            Err(_) => {
                // If evaluation fails, deny access for security
                false
            }
        }
    }

    /// Create sample row data for testing row-level security
    /// In a real implementation, this would come from the query engine
    fn create_sample_row_data(&self, resource: &Resource) -> HashMap<String, String> {
        let mut row_data = HashMap::new();
        
        // Generate realistic sample data based on resource
        match resource {
            Resource::Table { database, table, .. } => {
                match (database.as_str(), table.as_str()) {
                    ("sales", "orders") => {
                        row_data.insert("region".to_string(), "west".to_string());
                        row_data.insert("department".to_string(), "sales".to_string());
                        row_data.insert("customer_id".to_string(), "12345".to_string());
                        row_data.insert("amount".to_string(), "1000.00".to_string());
                        row_data.insert("status".to_string(), "active".to_string());
                    },
                    ("hr", "employees") => {
                        row_data.insert("department".to_string(), "engineering".to_string());
                        row_data.insert("manager".to_string(), "john_doe".to_string());
                        row_data.insert("level".to_string(), "senior".to_string());
                        row_data.insert("region".to_string(), "west".to_string());
                    },
                    ("finance", "transactions") => {
                        row_data.insert("classification".to_string(), "confidential".to_string());
                        row_data.insert("department".to_string(), "finance".to_string());
                        row_data.insert("region".to_string(), "east".to_string());
                    },
                    _ => {
                        // Default sample data
                        row_data.insert("region".to_string(), "west".to_string());
                        row_data.insert("department".to_string(), "general".to_string());
                    }
                }
            },
            Resource::Database { name } => {
                // Database-level filters might check metadata
                row_data.insert("database_owner".to_string(), "admin".to_string());
                row_data.insert("classification".to_string(), "internal".to_string());
                if name.contains("finance") {
                    row_data.insert("department".to_string(), "finance".to_string());
                }
            },
            _ => {
                // Default for other resource types
                row_data.insert("access_level".to_string(), "public".to_string());
            }
        }
        
        row_data
    }

    /// Get all effective permissions for a principal (including inherited)
    pub fn get_effective_permissions(&self, principal: &Principal) -> Vec<&Permission> {
        self.state.permissions
            .iter()
            .filter(|p| self.principal_matches(principal, &p.principal))
            .collect()
    }

    /// Check if a principal exists (user, role, group, etc.)
    pub fn principal_exists(&self, principal: &Principal) -> bool {
        match principal {
            Principal::Role(role_name) => self.state.roles.contains_key(role_name),
            Principal::User(_) => true, // Users always "exist" for now
            Principal::SamlGroup(_) => true, // Groups always "exist" for now
            Principal::ExternalAccount(_) => true, // External accounts always "exist"
            Principal::TaggedPrincipal { .. } => true, // Tagged principals always "exist"
        }
    }

    /// Add a user to a role
    pub fn add_user_to_role(&mut self, user: String, role: String) -> Result<(), String> {
        if let Some(members) = self.state.roles.get_mut(&role) {
            members.insert(user);
            Ok(())
        } else {
            Err(format!("Role '{}' does not exist", role))
        }
    }

    /// Remove a user from a role
    pub fn remove_user_from_role(&mut self, user: &str, role: &str) -> Result<(), String> {
        if let Some(members) = self.state.roles.get_mut(role) {
            members.remove(user);
            Ok(())
        } else {
            Err(format!("Role '{}' does not exist", role))
        }
    }

    /// Get all members of a role
    pub fn get_role_members(&self, role: &str) -> Option<&std::collections::HashSet<String>> {
        self.state.roles.get(role)
    }

    /// Check permissions with detailed reasoning (for debugging)
    pub fn check_permission_with_reason(
        &self, 
        principal: &Principal, 
        resource: &Resource, 
        action: &Action
    ) -> (bool, String) {
        let mut reasons = Vec::new();

        // Check each permission
        for (i, permission) in self.state.permissions.iter().enumerate() {
            let principal_match = self.principal_matches(principal, &permission.principal);
            let action_match = permission.actions.contains(action);
            let resource_match = resource.is_covered_by(&permission.resource);
            let row_filter_match = permission.row_filter.as_ref()
                .map(|f| self.evaluate_row_filter(f, resource))
                .unwrap_or(true);

            reasons.push(format!(
                "Permission {}: principal={} action={} resource={} row_filter={} => {}",
                i,
                principal_match,
                action_match,
                resource_match,
                row_filter_match,
                principal_match && action_match && resource_match && row_filter_match
            ));

            if principal_match && action_match && resource_match && row_filter_match {
                return (true, reasons.join("\n"));
            }
        }

        (false, format!("DENIED:\n{}", reasons.join("\n")))
    }
}

impl Default for EmulatorEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_direct_permission_check() {
        let mut engine = EmulatorEngine::new();
        
        // Create a permission
        let permission = Permission {
            principal: Principal::Role("analyst".to_string()),
            resource: Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            actions: vec![Action::Select, Action::Insert],
            grant_option: false,
            row_filter: None,
        };

        let mut state = EmulatorState::new();
        state.permissions.push(permission);
        engine.update_state(&state);

        // Test allowed action
        let allowed = engine.check_permission(
            &Principal::Role("analyst".to_string()),
            &Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            &Action::Select
        );
        assert!(allowed);

        // Test denied action
        let denied = engine.check_permission(
            &Principal::Role("analyst".to_string()),
            &Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            &Action::Delete
        );
        assert!(!denied);
    }

    #[test]
    fn test_role_membership() {
        let mut engine = EmulatorEngine::new();
        let mut state = EmulatorState::new();
        
        // Create role with member
        let mut members = HashSet::new();
        members.insert("john@company.com".to_string());
        state.roles.insert("analyst".to_string(), members);
        
        // Create permission for role
        let permission = Permission {
            principal: Principal::Role("analyst".to_string()),
            resource: Resource::Database {
                name: "sales".to_string(),
            },
            actions: vec![Action::Select],
            grant_option: false,
            row_filter: None,
        };
        state.permissions.push(permission);
        
        engine.update_state(&state);

        // User should have permission through role membership
        let allowed = engine.check_permission(
            &Principal::User("john@company.com".to_string()),
            &Resource::Database {
                name: "sales".to_string(),
            },
            &Action::Select
        );
        assert!(allowed);

        // Non-member should not have permission
        let denied = engine.check_permission(
            &Principal::User("jane@company.com".to_string()),
            &Resource::Database {
                name: "sales".to_string(),
            },
            &Action::Select
        );
        assert!(!denied);
    }

    #[test]
    fn test_permission_reasoning() {
        let mut engine = EmulatorEngine::new();
        let mut state = EmulatorState::new();

        let permission = Permission {
            principal: Principal::Role("analyst".to_string()),
            resource: Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            actions: vec![Action::Select],
            grant_option: false,
            row_filter: None,
        };
        state.permissions.push(permission);
        engine.update_state(&state);

        let (allowed, reason) = engine.check_permission_with_reason(
            &Principal::Role("different_role".to_string()),
            &Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            &Action::Select
        );

        assert!(!allowed);
        assert!(reason.contains("DENIED"));
        assert!(reason.contains("principal=false"));
    }
}