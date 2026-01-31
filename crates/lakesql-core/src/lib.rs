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
    use super::*;

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