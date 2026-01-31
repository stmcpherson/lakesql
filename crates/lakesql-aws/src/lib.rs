//! # AWS Lake Formation Backend
//! 
//! Real AWS Lake Formation implementation for production usage.

use aws_config::{BehaviorVersion, Region};
use aws_sdk_lakeformation::{Client, Config};
use aws_sdk_lakeformation::types::{
    DataLakeSettings, DataLakePrincipal, Resource as LfResource,
    Permission as LfPermission, LfTag as AwsLfTag
};
use lakesql_core::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashMap;

/// AWS Lake Formation backend implementation
pub struct AwsBackend {
    client: Client,
    region: String,
}

impl AwsBackend {
    /// Create new AWS backend with default config
    pub async fn new() -> Result<Self> {
        Self::with_config(None, None, None).await
    }

    /// Create AWS backend with custom configuration
    pub async fn with_config(
        region: Option<String>,
        profile: Option<String>,
        endpoint: Option<String>,
    ) -> Result<Self> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());

        // Set region if provided
        if let Some(region) = &region {
            loader = loader.region(Region::new(region.clone()));
        }

        // Set profile if provided
        if let Some(profile) = profile {
            loader = loader.profile_name(&profile);
        }

        let aws_config = loader.load().await;

        // Create Lake Formation client
        let mut lf_config = Config::from(&aws_config);
        
        // Set custom endpoint if provided (for LocalStack testing)
        if let Some(endpoint) = endpoint {
            lf_config = lf_config.endpoint_url(endpoint);
        }

        let client = Client::from_conf(lf_config);
        
        let region_name = aws_config
            .region()
            .map(|r| r.as_ref().to_string())
            .unwrap_or_else(|| "us-east-1".to_string());

        Ok(Self {
            client,
            region: region_name,
        })
    }
}

#[async_trait]
impl LakeFormationBackend for AwsBackend {
    async fn execute_ddl(&mut self, sql: &str) -> Result<DdlResult> {
        // Parse the SQL and route to appropriate method
        let parsed = lakesql_parser::parse_ddl(sql)?;
        
        match parsed {
            DdlStatement::Grant { permission } => {
                self.grant_permissions(permission).await
            }
            DdlStatement::Revoke { principal, resource, actions } => {
                self.revoke_permissions(&principal, &resource, &actions).await
            }
            DdlStatement::CreateRole { role_name, .. } => {
                // Lake Formation doesn't have explicit role creation
                // Roles are implicit when first used
                Ok(DdlResult::Success {
                    message: format!("Role '{}' will be created implicitly when first used", role_name),
                    rows_affected: 0,
                })
            }
            DdlStatement::CreateTag { tag } => {
                self.create_tag(tag).await
            }
            DdlStatement::DropTag { tag_key } => {
                self.delete_tag(&tag_key).await
            }
        }
    }

    async fn grant_permissions(&mut self, permission: Permission) -> Result<DdlResult> {
        let principal = convert_principal(&permission.principal)?;
        let resource = convert_resource(&permission.resource)?;
        let permissions = convert_actions(&permission.actions);

        let request = self.client
            .grant_permissions()
            .principal(principal)
            .resource(resource)
            .set_permissions(Some(permissions));

        // Add grant option if specified
        let request = if permission.grant_option {
            request.set_permissions_with_grant_option(Some(convert_actions(&permission.actions)))
        } else {
            request
        };

        match request.send().await {
            Ok(_) => Ok(DdlResult::Success {
                message: format!("Granted permissions successfully"),
                rows_affected: 1,
            }),
            Err(e) => Err(anyhow!("Failed to grant permissions: {}", e)),
        }
    }

    async fn revoke_permissions(
        &mut self,
        principal: &Principal,
        resource: &Resource,
        actions: &[Action],
    ) -> Result<DdlResult> {
        let aws_principal = convert_principal(principal)?;
        let aws_resource = convert_resource(resource)?;
        let aws_permissions = convert_actions(actions);

        match self.client
            .revoke_permissions()
            .principal(aws_principal)
            .resource(aws_resource)
            .set_permissions(Some(aws_permissions))
            .send()
            .await
        {
            Ok(_) => Ok(DdlResult::Success {
                message: format!("Revoked permissions successfully"),
                rows_affected: 1,
            }),
            Err(e) => Err(anyhow!("Failed to revoke permissions: {}", e)),
        }
    }

    async fn check_permissions(
        &self,
        principal: &Principal,
        resource: &Resource,
        action: &Action,
    ) -> Result<bool> {
        let aws_principal = convert_principal(principal)?;
        let aws_resource = convert_resource(resource)?;

        let response = self.client
            .get_effective_permissions_for_path()
            .resource_arn(get_resource_arn(resource, &self.region)?)
            .send()
            .await?;

        // Check if the principal has the required permission
        if let Some(permissions) = response.permissions_by_principal {
            for permission_entry in permissions {
                if is_principal_match(&permission_entry.principal, &aws_principal) {
                    if let Some(perms) = permission_entry.permissions {
                        for perm in perms {
                            if is_action_match(&perm, action) {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    async fn create_tag(&mut self, tag: LfTag) -> Result<DdlResult> {
        let aws_tag = AwsLfTag::builder()
            .tag_key(&tag.key)
            .set_tag_values(Some(tag.values))
            .build()
            .map_err(|e| anyhow!("Failed to build LF-Tag: {}", e))?;

        match self.client
            .create_lf_tag()
            .tag_key(&tag.key)
            .set_tag_values(Some(tag.values))
            .send()
            .await
        {
            Ok(_) => Ok(DdlResult::Success {
                message: format!("Created LF-Tag '{}' successfully", tag.key),
                rows_affected: 1,
            }),
            Err(e) => Err(anyhow!("Failed to create LF-Tag: {}", e)),
        }
    }

    async fn delete_tag(&mut self, tag_key: &str) -> Result<DdlResult> {
        match self.client
            .delete_lf_tag()
            .tag_key(tag_key)
            .send()
            .await
        {
            Ok(_) => Ok(DdlResult::Success {
                message: format!("Deleted LF-Tag '{}' successfully", tag_key),
                rows_affected: 1,
            }),
            Err(e) => Err(anyhow!("Failed to delete LF-Tag: {}", e)),
        }
    }

    async fn list_permissions_for_principal(
        &self,
        principal: &Principal,
    ) -> Result<Vec<Permission>> {
        let aws_principal = convert_principal(principal)?;

        let response = self.client
            .list_permissions()
            .principal(aws_principal)
            .send()
            .await?;

        let mut permissions = Vec::new();
        
        if let Some(principal_resource_permissions) = response.principal_resource_permissions {
            for perm_entry in principal_resource_permissions {
                if let Some(resource) = perm_entry.resource {
                    if let Some(perms) = perm_entry.permissions {
                        let actions: Vec<Action> = perms
                            .iter()
                            .filter_map(|p| convert_aws_permission_to_action(p))
                            .collect();

                        if !actions.is_empty() {
                            permissions.push(Permission {
                                principal: principal.clone(),
                                resource: convert_aws_resource_to_resource(&resource)?,
                                actions,
                                grant_option: perm_entry.permissions_with_grant_option.is_some(),
                                row_filter: None,
                            });
                        }
                    }
                }
            }
        }

        Ok(permissions)
    }

    async fn list_permissions_for_resource(&self, resource: &Resource) -> Result<Vec<Permission>> {
        let resource_arn = get_resource_arn(resource, &self.region)?;

        let response = self.client
            .get_effective_permissions_for_path()
            .resource_arn(&resource_arn)
            .send()
            .await?;

        let mut permissions = Vec::new();

        if let Some(permissions_by_principal) = response.permissions_by_principal {
            for perm_entry in permissions_by_principal {
                if let Some(principal) = perm_entry.principal {
                    if let Some(perms) = perm_entry.permissions {
                        let actions: Vec<Action> = perms
                            .iter()
                            .filter_map(|p| convert_aws_permission_to_action(p))
                            .collect();

                        if !actions.is_empty() {
                            permissions.push(Permission {
                                principal: convert_aws_principal_to_principal(&principal)?,
                                resource: resource.clone(),
                                actions,
                                grant_option: false, // TODO: Check grant options properly
                                row_filter: None,
                            });
                        }
                    }
                }
            }
        }

        Ok(permissions)
    }

    async fn set_session_context(&mut self, _context: HashMap<String, String>) -> Result<()> {
        // Lake Formation doesn't have a direct session context concept
        // This would be handled at the query execution level
        Ok(())
    }
}

// Helper functions for converting between our types and AWS SDK types

fn convert_principal(principal: &Principal) -> Result<DataLakePrincipal> {
    match principal {
        Principal::User(arn) | Principal::Role(arn) => {
            Ok(DataLakePrincipal::builder()
                .data_lake_principal_identifier(arn)
                .build())
        }
        Principal::ExternalAccount(account_id) => {
            Ok(DataLakePrincipal::builder()
                .data_lake_principal_identifier(account_id)
                .build())
        }
        Principal::SamlGroup(group) => {
            Ok(DataLakePrincipal::builder()
                .data_lake_principal_identifier(group)
                .build())
        }
        Principal::TaggedPrincipal { .. } => {
            Err(anyhow!("Tagged principals not yet supported in AWS backend"))
        }
    }
}

fn convert_resource(resource: &Resource) -> Result<LfResource> {
    match resource {
        Resource::Database { name } => {
            Ok(LfResource::builder()
                .database(
                    aws_sdk_lakeformation::types::DatabaseResource::builder()
                        .name(name)
                        .build()
                        .map_err(|e| anyhow!("Failed to build database resource: {}", e))?
                )
                .build())
        }
        Resource::Table { database, table, columns } => {
            let table_resource = aws_sdk_lakeformation::types::TableResource::builder()
                .database_name(database)
                .name(table);

            let table_resource = if let Some(cols) = columns {
                table_resource.set_column_names(Some(cols.clone()))
            } else {
                table_resource
            };

            Ok(LfResource::builder()
                .table(table_resource.build().map_err(|e| anyhow!("Failed to build table resource: {}", e))?)
                .build())
        }
        Resource::DataLocation { path } => {
            Ok(LfResource::builder()
                .data_location(
                    aws_sdk_lakeformation::types::DataLocationResource::builder()
                        .resource_arn(path)
                        .build()
                        .map_err(|e| anyhow!("Failed to build data location resource: {}", e))?
                )
                .build())
        }
        Resource::TaggedResource { .. } => {
            Err(anyhow!("Tagged resources not yet supported in AWS backend"))
        }
    }
}

fn convert_actions(actions: &[Action]) -> Vec<LfPermission> {
    actions.iter().map(|action| match action {
        Action::Select => LfPermission::Select,
        Action::Insert => LfPermission::Insert,
        Action::Update => LfPermission::Insert, // Lake Formation doesn't have UPDATE
        Action::Delete => LfPermission::Delete,
        Action::Create => LfPermission::CreateTable,
        Action::Alter => LfPermission::Alter,
        Action::Drop => LfPermission::Drop,
    }).collect()
}

// Helper functions for reverse conversion (AWS -> our types)

fn convert_aws_principal_to_principal(aws_principal: &DataLakePrincipal) -> Result<Principal> {
    if let Some(identifier) = &aws_principal.data_lake_principal_identifier {
        if identifier.starts_with("arn:aws:iam::") {
            if identifier.contains(":user/") {
                Ok(Principal::User(identifier.clone()))
            } else if identifier.contains(":role/") {
                Ok(Principal::Role(identifier.clone()))
            } else {
                Ok(Principal::ExternalAccount(identifier.clone()))
            }
        } else {
            Ok(Principal::SamlGroup(identifier.clone()))
        }
    } else {
        Err(anyhow!("Invalid AWS principal: missing identifier"))
    }
}

fn convert_aws_resource_to_resource(aws_resource: &LfResource) -> Result<Resource> {
    if let Some(db) = &aws_resource.database {
        Ok(Resource::Database {
            name: db.name.clone().unwrap_or_default(),
        })
    } else if let Some(table) = &aws_resource.table {
        Ok(Resource::Table {
            database: table.database_name.clone().unwrap_or_default(),
            table: table.name.clone().unwrap_or_default(),
            columns: table.column_names.clone(),
        })
    } else if let Some(data_loc) = &aws_resource.data_location {
        Ok(Resource::DataLocation {
            path: data_loc.resource_arn.clone().unwrap_or_default(),
        })
    } else {
        Err(anyhow!("Unsupported AWS resource type"))
    }
}

fn convert_aws_permission_to_action(aws_perm: &LfPermission) -> Option<Action> {
    match aws_perm {
        LfPermission::Select => Some(Action::Select),
        LfPermission::Insert => Some(Action::Insert),
        LfPermission::Delete => Some(Action::Delete),
        LfPermission::CreateTable => Some(Action::Create),
        LfPermission::Alter => Some(Action::Alter),
        LfPermission::Drop => Some(Action::Drop),
        _ => None,
    }
}

fn get_resource_arn(resource: &Resource, region: &str) -> Result<String> {
    match resource {
        Resource::Database { name } => {
            Ok(format!("arn:aws:lakeformation:{}:*:database/{}", region, name))
        }
        Resource::Table { database, table, .. } => {
            Ok(format!("arn:aws:lakeformation:{}:*:table/{}/{}", region, database, table))
        }
        Resource::DataLocation { path } => {
            Ok(path.clone())
        }
        Resource::TaggedResource { .. } => {
            Err(anyhow!("Tagged resources not supported for ARN generation"))
        }
    }
}

fn is_principal_match(
    aws_principal: &Option<DataLakePrincipal>,
    target_principal: &DataLakePrincipal,
) -> bool {
    if let Some(principal) = aws_principal {
        principal.data_lake_principal_identifier == target_principal.data_lake_principal_identifier
    } else {
        false
    }
}

fn is_action_match(aws_permission: &LfPermission, target_action: &Action) -> bool {
    matches!(
        (aws_permission, target_action),
        (LfPermission::Select, Action::Select) |
        (LfPermission::Insert, Action::Insert) |
        (LfPermission::Delete, Action::Delete) |
        (LfPermission::CreateTable, Action::Create) |
        (LfPermission::Alter, Action::Alter) |
        (LfPermission::Drop, Action::Drop)
    )
}

// Export the main constructor
pub async fn create_aws_backend(
    region: Option<String>,
    profile: Option<String>,
    endpoint: Option<String>,
) -> Result<AwsBackend> {
    AwsBackend::with_config(region, profile, endpoint).await
}