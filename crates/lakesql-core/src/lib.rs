//! # LakeSQL Core
//! 
//! Core types and traits for Lake Formation DDL operations.

pub mod types;
pub mod permissions;
pub mod backend;

pub use types::*;
pub use permissions::*;
pub use backend::*;

#[cfg(test)]
mod tests {
    #[test]
    fn test_invalid_permission_denied() {
        use lakesql_types::{Permission, Principal, Resource, Action};
        use crate::permissions::PermissionEngine;

        let mut engine = PermissionEngine::new();

        // No permissions granted
        let denied = engine.check_permission(
            &Principal::User("alice@company.com".to_string()),
            &Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            &Action::Select,
        );
        assert!(!denied);
    }

    #[test]
    fn test_malformed_resource_query() {
        use lakesql_types::{Permission, Principal, Resource, Action};
        use crate::permissions::PermissionEngine;

        let mut engine = PermissionEngine::new();

        // Grant permission to user for a valid table
        let perm = Permission {
            principal: Principal::User("bob@company.com".to_string()),
            resource: Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            actions: vec![Action::Select],
            grant_option: false,
            row_filter: None,
        };
        engine.grant_permission(perm).unwrap();

        // Query for a non-existent table
        let denied = engine.check_permission(
            &Principal::User("bob@company.com".to_string()),
            &Resource::Table {
                database: "sales".to_string(),
                table: "nonexistent".to_string(),
                columns: None,
            },
            &Action::Select,
        );
        assert!(!denied);
    }
    #[test]
    fn test_cross_account_permission() {
        use lakesql_types::{Permission, Principal, Resource, Action};
        use crate::permissions::PermissionEngine;

        let mut engine = PermissionEngine::new();

        // Grant permission to external account
        let perm = Permission {
            principal: Principal::ExternalAccount("accountB".to_string()),
            resource: Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            actions: vec![Action::Select],
            grant_option: false,
            row_filter: None,
        };
        engine.grant_permission(perm).unwrap();

        // Should allow access for accountB
        let allowed = engine.check_permission(
            &Principal::ExternalAccount("accountB".to_string()),
            &Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            &Action::Select,
        );
        assert!(allowed);

        // Should deny access for accountA
        let denied = engine.check_permission(
            &Principal::ExternalAccount("accountA".to_string()),
            &Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            &Action::Select,
        );
        assert!(!denied);
    }
    #[test]
    fn test_column_level_security() {
        use lakesql_types::{Permission, Principal, Resource, Action};
        use crate::permissions::PermissionEngine;

        let mut engine = PermissionEngine::new();

        // Grant permission to user for specific columns
        let perm = Permission {
            principal: Principal::User("bob@company.com".to_string()),
            resource: Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: Some(vec!["customer_id".to_string(), "amount".to_string()]),
            },
            actions: vec![Action::Select],
            grant_option: false,
            row_filter: None,
        };
        engine.grant_permission(perm).unwrap();

        // Should allow access to permitted columns
        let allowed = engine.check_permission(
            &Principal::User("bob@company.com".to_string()),
            &Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: Some(vec!["customer_id".to_string()]),
            },
            &Action::Select,
        );
        assert!(allowed);

        // Should deny access to non-permitted columns
        let denied = engine.check_permission(
            &Principal::User("bob@company.com".to_string()),
            &Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: Some(vec!["status".to_string()]),
            },
            &Action::Select,
        );
        assert!(!denied);
    }
    #[test]
    fn test_tag_based_access_control() {
        use lakesql_types::{Permission, Principal, Resource, Action, LfTag};
        use crate::permissions::PermissionEngine;

        let tag = LfTag {
            key: "department".to_string(),
            values: vec!["finance".to_string(), "marketing".to_string()],
            description: Some("Department tag".to_string()),
        };

        let mut engine = PermissionEngine::new();
        engine.create_tag(tag.clone()).unwrap();

        // Grant permission to principal with matching tag
        let perm = Permission {
            principal: Principal::TaggedPrincipal {
                tag_key: "department".to_string(),
                tag_values: vec!["finance".to_string()],
            },
            resource: Resource::TaggedResource {
                tag_conditions: vec![("department".to_string(), vec!["finance".to_string()])],
            },
            actions: vec![Action::Select],
            grant_option: false,
            row_filter: None,
        };
    }
    #[test]
    fn test_grant_select_permission() {
        let perm = Permission {
            principal: Principal::Role("data_scientist".to_string()),
            resource: Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            actions: vec![Action::Select],
            grant_option: false,
            row_filter: None,
        };
        assert_eq!(perm.actions, vec![Action::Select]);
        assert_eq!(perm.principal, Principal::Role("data_scientist".to_string()));
    }

    #[test]
    fn test_grant_all_on_database_to_user() {
        let perm = Permission {
            principal: Principal::User("alice@company.com".to_string()),
            resource: Resource::Database { name: "analytics".to_string() },
            actions: vec![Action::Select, Action::Insert, Action::Update, Action::Delete, Action::CreateTable, Action::AlterTable, Action::DropTable],
            grant_option: true,
            row_filter: None,
        };
    }
    use lakesql_types::{Permission, Principal, Resource, Action};

    #[test] 
    fn test_basic_permission() {
        let perm = Permission {
            principal: Principal::Role("data_scientist".to_string()),
            resource: Resource::Table {
                database: "sales".to_string(),
                table: "orders".to_string(),
                columns: None,
            },
            actions: vec![Action::Select],
            grant_option: false,
            row_filter: None,
        };
        
        assert_eq!(perm.actions.len(), 1);
        assert_eq!(perm.actions[0], Action::Select);
    }
}
