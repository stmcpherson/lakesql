// Integration tests for lakesql-cli
// Covers permission management, query execution, and error reporting via CLI

use assert_cmd::Command;
use assert_cmd::cargo::cargo_bin_cmd;
use std::env;
use tempfile::NamedTempFile;
use predicates::str::contains;

#[test]
fn test_execute_create_role() {
    let state_file = NamedTempFile::new().unwrap();
    env::set_var("LAKESQL_TEST_STATE_FILE", state_file.path());
    let mut cmd = cargo_bin_cmd!("lakesql-cli");
    cmd.arg("execute").arg("--sql").arg("CREATE ROLE test_role");
    cmd.assert()
        .success()
        .stdout(contains("Success"));
}

#[test]
fn test_check_permission_allowed() {
    let state_file = NamedTempFile::new().unwrap();
    env::set_var("LAKESQL_TEST_STATE_FILE", state_file.path());
    // Setup: create role and grant permission
    let mut cmd = cargo_bin_cmd!("lakesql-cli");
    cmd.arg("execute").arg("--sql").arg("CREATE ROLE test_user");
    cmd.assert().success();
    let mut cmd2 = cargo_bin_cmd!("lakesql-cli");
    cmd2.arg("execute").arg("--sql").arg("GRANT SELECT ON sales.orders TO ROLE test_user");
    cmd2.assert().success();
    // Check permission
    let mut cmd3 = cargo_bin_cmd!("lakesql-cli");
    cmd3.arg("check")
        .arg("--principal").arg("ROLE test_user")
        .arg("--resource").arg("sales.orders")
        .arg("--action").arg("SELECT");
    cmd3.assert()
        .success()
        .stdout(contains("ALLOWED"));
}

#[test]
fn test_check_permission_denied() {
    let state_file = NamedTempFile::new().unwrap();
    env::set_var("LAKESQL_TEST_STATE_FILE", state_file.path());
    let mut cmd = cargo_bin_cmd!("lakesql-cli");
    cmd.arg("check")
        .arg("--principal").arg("ROLE nobody")
        .arg("--resource").arg("sales.orders")
        .arg("--action").arg("SELECT");
    cmd.assert()
        .success()
        .stdout(contains("DENIED"));
}

#[test]
fn test_execute_invalid_sql() {
    let state_file = NamedTempFile::new().unwrap();
    env::set_var("LAKESQL_TEST_STATE_FILE", state_file.path());
    let mut cmd = cargo_bin_cmd!("lakesql-cli");
    cmd.arg("execute").arg("--sql").arg("INVALID SQL STATEMENT");
    cmd.assert()
        .failure()
        .stdout(contains("Parse error"));
}
