//! Core data types for Lake Formation DDL

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a principal (user, role, group) that can have permissions
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Principal {
    /// IAM User (arn:aws:iam::123456789012:user/alice)
    User(String),
    /// IAM Role (arn:aws:iam::123456789012:role/data-scientist) 
    Role(String),
    /// SAML Group from external identity provider
    SamlGroup(String),
    /// Cross-account external principal
    ExternalAccount(String),
    /// Lake Formation tag-based principal
    TaggedPrincipal {
        tag_key: String,
        tag_values: Vec<String>,
    },
}

/// Represents a data resource that can be protected
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resource {
    /// Entire database
    Database {
        name: String,
    },
    /// Specific table, optionally with column restrictions
    Table {
        database: String,
        table: String,
        columns: Option<Vec<String>>,
    },
    /// Data location (S3 path)
    DataLocation {
        path: String,
    },
    /// Resources matching LF-Tags (using Vec of tuples for Hash compatibility)
    TaggedResource {
        tag_conditions: Vec<(String, Vec<String>)>,
    },
}

// Manual Hash implementation for Resource
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
                // Sort for consistent hashing
                let mut sorted_conditions = tag_conditions.clone();
                sorted_conditions.sort();
                sorted_conditions.hash(state);
            },
        }
    }
}

/// Actions that can be granted on resources
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    // Table-level permissions
    Select,
    Insert,
    Update, 
    Delete,
    
    // Database-level permissions  
    CreateTable,
    DropTable,
    AlterTable,
    Describe,
    
    // Data location permissions
    DataLocationAccess,
    
    // Administrative permissions
    GrantWithGrantOption,
}

/// Row-level security filter expression
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowFilter {
    pub expression: String,
    pub session_context: Option<HashMap<String, String>>,
}

/// A complete permission grant/revoke
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)] 
pub struct Permission {
    pub principal: Principal,
    pub resource: Resource,
    pub actions: Vec<Action>,
    pub grant_option: bool,
    pub row_filter: Option<RowFilter>,
}

/// Lake Formation Tag definition
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LfTag {
    pub key: String,
    pub values: Vec<String>,
    pub description: Option<String>,
}

/// Results from DDL execution  
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
    /// Check if this principal matches another (for permission resolution)
    pub fn matches(&self, other: &Principal) -> bool {
        match (self, other) {
            (Principal::User(a), Principal::User(b)) => a == b,
            (Principal::Role(a), Principal::Role(b)) => a == b,
            (Principal::SamlGroup(a), Principal::SamlGroup(b)) => a == b,
            (Principal::ExternalAccount(a), Principal::ExternalAccount(b)) => a == b,
            // Tagged principals require more complex matching logic
            _ => false,
        }
    }
}

impl Resource {
    /// Check if this resource is contained within or matches another resource
    pub fn is_covered_by(&self, other: &Resource) -> bool {
        match (self, other) {
            // Exact table match
            (Resource::Table { database: db1, table: t1, .. }, 
             Resource::Table { database: db2, table: t2, .. }) => {
                db1 == db2 && t1 == t2
            },
            
            // Table is covered by database permission
            (Resource::Table { database: db1, .. }, 
             Resource::Database { name: db2 }) => {
                db1 == db2
            },
            
            // Exact database match
            (Resource::Database { name: db1 }, 
             Resource::Database { name: db2 }) => {
                db1 == db2
            },
            
            // Data location prefix matching
            (Resource::DataLocation { path: p1 },
             Resource::DataLocation { path: p2 }) => {
                p1.starts_with(p2) || p1 == p2
            },
            
            _ => false,
        }
    }
}