// Integration tests for lakesql-cli
// Covers permission management, query execution, and error reporting via CLI

use assert_cmd::Command;
use predicates::str::contains;

#[test]
fn test_execute_create_role() {
    let mut cmd = Command::cargo_bin("lakesql-cli").unwrap();
    cmd.arg("execute").arg("--sql").arg("CREATE ROLE test_role");
    cmd.assert()
        .success()
        .stdout(contains("Success"));
}

#[test]
fn test_check_permission_allowed() {
    let mut cmd = Command::cargo_bin("lakesql-cli").unwrap();
    // Setup: create role and grant permission
    cmd.arg("execute").arg("--sql").arg("CREATE ROLE test_user");
    cmd.assert().success();
    let mut cmd2 = Command::cargo_bin("lakesql-cli").unwrap();
    cmd2.arg("execute").arg("--sql").arg("GRANT SELECT ON sales.orders TO ROLE test_user");
    cmd2.assert().success();
    // Check permission
    let mut cmd3 = Command::cargo_bin("lakesql-cli").unwrap();
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
    let mut cmd = Command::cargo_bin("lakesql-cli").unwrap();
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
    let mut cmd = Command::cargo_bin("lakesql-cli").unwrap();
    cmd.arg("execute").arg("--sql").arg("INVALID SQL STATEMENT");
    cmd.assert()
        .failure()
        .stdout(contains("Error"));
}
