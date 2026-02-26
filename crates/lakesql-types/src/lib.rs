use async_trait::async_trait;
use anyhow::Result;

#[async_trait]
pub trait LakeFormationBackend: Send + Sync {
    async fn execute_ddl(&mut self, sql: &str) -> Result<DdlResult>;
    async fn grant_permissions(&mut self, permission: Permission) -> Result<DdlResult>;
    async fn revoke_permissions(&mut self, principal: &Principal, resource: &Resource, actions: &[Action]) -> Result<DdlResult>;
    async fn check_permissions(&self, principal: &Principal, resource: &Resource, action: &Action) -> Result<bool>;
    async fn create_tag(&mut self, tag: LfTag) -> Result<DdlResult>;
    async fn delete_tag(&mut self, tag_key: &str) -> Result<DdlResult>;
    async fn list_permissions_for_principal(&self, principal: &Principal) -> Result<Vec<Permission>>;
    async fn list_permissions_for_resource(&self, resource: &Resource) -> Result<Vec<Permission>>;
    async fn set_session_context(&mut self, context: std::collections::HashMap<String, String>) -> Result<()>;
}
/// Shared types and traits for LakeSQL

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Principal {
    User(String),
    Role(String),
    SamlGroup(String),
    ExternalAccount(String),
    TaggedPrincipal {
        tag_key: String,
        tag_values: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resource {
    Database {
        name: String,
    },
    Table {
        database: String,
        table: String,
        columns: Option<Vec<String>>,
    },
    DataLocation {
        path: String,
    },
    TaggedResource {
        tag_conditions: Vec<(String, Vec<String>)>,
    },
}

impl std::hash::Hash for Resource {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Resource::Database { name } => {
                0.hash(state);
                name.hash(state);
            },
            Resource::Table { database, table, columns } => {
                1.hash(state);
                database.hash(state);
                table.hash(state);
                columns.hash(state);
            },
            Resource::DataLocation { path } => {
                2.hash(state);
                path.hash(state);
            },
            Resource::TaggedResource { tag_conditions } => {
                3.hash(state);
                let mut sorted_conditions = tag_conditions.clone();
                sorted_conditions.sort();
                sorted_conditions.hash(state);
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    AlterTable,
    Describe,
    DataLocationAccess,
    GrantWithGrantOption,
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowFilter {
    pub expression: String,
    pub session_context: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Permission {
    pub principal: Principal,
    pub resource: Resource,
    pub actions: Vec<Action>,
    pub grant_option: bool,
    pub row_filter: Option<RowFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LfTag {
    pub key: String,
    pub values: Vec<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DdlResult {
    Success { message: String },
    Error { error: String },
    PermissionCheck {
        allowed: bool,
        reason: Option<String>
    },
}

impl Principal {
    pub fn matches(&self, other: &Principal) -> bool {
        match (self, other) {
            (Principal::User(a), Principal::User(b)) => a == b,
            (Principal::Role(a), Principal::Role(b)) => a == b,
            (Principal::SamlGroup(a), Principal::SamlGroup(b)) => a == b,
            (Principal::ExternalAccount(a), Principal::ExternalAccount(b)) => a == b,
            _ => false,
        }
    }
}

impl Resource {
    pub fn is_covered_by(&self, other: &Resource) -> bool {
        match (self, other) {
            (
                Resource::Table { database: db1, table: t1, columns: Some(requested_cols) },
                Resource::Table { database: db2, table: t2, columns: Some(permitted_cols) }
            ) => {
                db1 == db2 && t1 == t2 && requested_cols.iter().all(|c| permitted_cols.contains(c))
            },
            (
                Resource::Table { database: db1, table: t1, columns: None },
                Resource::Table { database: db2, table: t2, columns: Some(_permitted_cols) }
            ) => {
                // Requesting all columns, but permission is only for some columns: deny
                db1 == db2 && t1 == t2 && false
            },
            (
                Resource::Table { database: db1, table: t1, columns: Some(requested_cols) },
                Resource::Table { database: db2, table: t2, columns: None }
            ) => {
                // Permission is for all columns, allow any subset
                db1 == db2 && t1 == t2
            },
            (Resource::Table { database: db1, table: t1, .. }, Resource::Table { database: db2, table: t2, .. }) => db1 == db2 && t1 == t2,
            (Resource::Table { database: db1, .. }, Resource::Database { name: db2 }) => db1 == db2,
            (Resource::Database { name: db1 }, Resource::Database { name: db2 }) => db1 == db2,
            (Resource::DataLocation { path: p1 }, Resource::DataLocation { path: p2 }) => p1.starts_with(p2) || p1 == p2,
            _ => false,
        }
    }
}
