use async_trait::async_trait;
/// Backend trait for different Lake Formation implementations

use lakesql_types::{DdlResult, Principal, Resource, Action, LfTag, Permission, LakeFormationBackend};
use anyhow::anyhow;
use anyhow::Result;


/// Configuration for backend implementations
#[derive(Debug, Clone)]
pub enum BackendConfig {
    Emulator {
        state_file: Option<String>,
    },
    Aws {
        region: Option<String>,
        profile: Option<String>,
        endpoint: Option<String>,
    },
}

/// Factory for creating backend instances
pub struct BackendFactory;

impl BackendFactory {
    pub async fn create(config: BackendConfig) -> Result<Box<dyn LakeFormationBackend>> {
        match config {
            BackendConfig::Emulator { state_file } => {
                let emulator = crate::create_emulator_backend(state_file).await?;
                Ok(Box::new(emulator))
            },
            BackendConfig::Aws { .. } => {
                Err(anyhow!("AWS backend is not available in lakesql-core. Use lakesql-aws crate directly."))
            },
        }
    }
}

/// Placeholder backend for when features are not enabled
pub struct PlaceholderBackend;

#[async_trait]
impl LakeFormationBackend for PlaceholderBackend {
    async fn execute_ddl(&mut self, _sql: &str) -> Result<DdlResult> {
        Err(anyhow!("No backend configured"))
    }
    
    async fn grant_permissions(&mut self, _permission: Permission) -> Result<DdlResult> {
        Err(anyhow!("No backend configured"))
    }
    
    async fn revoke_permissions(&mut self, _principal: &Principal, _resource: &Resource, _actions: &[Action]) -> Result<DdlResult> {
        Err(anyhow!("No backend configured"))
    }
    
    async fn check_permissions(&self, _principal: &Principal, _resource: &Resource, _action: &Action) -> Result<bool> {
        Err(anyhow!("No backend configured"))
    }
    
    async fn create_tag(&mut self, _tag: LfTag) -> Result<DdlResult> {
        Err(anyhow!("No backend configured"))
    }
    
    async fn delete_tag(&mut self, _tag_key: &str) -> Result<DdlResult> {
        Err(anyhow!("No backend configured"))
    }
    
    async fn list_permissions_for_principal(&self, _principal: &Principal) -> Result<Vec<Permission>> {
        Err(anyhow!("No backend configured"))
    }
    
    async fn list_permissions_for_resource(&self, _resource: &Resource) -> Result<Vec<Permission>> {
        Err(anyhow!("No backend configured"))
    }
    
    async fn set_session_context(&mut self, _context: std::collections::HashMap<String, String>) -> Result<()> {
        Err(anyhow!("No backend configured"))
    }
}

/// Create an emulator backend (stub - use lakesql-emulator crate directly)
pub async fn create_emulator_backend(
    _state_file: Option<String>,
) -> Result<PlaceholderBackend> {
    Err(anyhow!("Emulator backend not compiled - enable 'emulator' feature"))
}
