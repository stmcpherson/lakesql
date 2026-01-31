//! Expression evaluation engine for row-level security filters

use lakesql_core::*;
use std::collections::HashMap;
use anyhow::{Result, anyhow};

/// Simple expression evaluator for row-level security
#[derive(Debug, Clone)]
pub struct ExpressionEvaluator {
    /// Available session context
    session_context: HashMap<String, String>,
    /// Sample row data for evaluation
    row_data: HashMap<String, String>,
}

impl ExpressionEvaluator {
    pub fn new() -> Self {
        Self {
            session_context: HashMap::new(),
            row_data: HashMap::new(),
        }
    }

    /// Set session context (like current user's region, department, etc.)
    pub fn set_session_context(&mut self, context: HashMap<String, String>) {
        self.session_context = context;
    }

    /// Set current row data for evaluation
    pub fn set_row_data(&mut self, row: HashMap<String, String>) {
        self.row_data = row;
    }

    /// Evaluate a row filter expression
    pub fn evaluate_filter(&self, filter: &RowFilter) -> Result<bool> {
        // For now, do simple string-based evaluation
        // In a real implementation, you'd parse this into an AST
        self.evaluate_expression(&filter.expression)
    }

    /// Evaluate a simple expression (basic implementation)
    fn evaluate_expression(&self, expr: &str) -> Result<bool> {
        let expr = expr.trim();
        
        // Handle WHERE keyword
        let expr = if expr.to_uppercase().starts_with("WHERE ") {
            &expr[6..]
        } else {
            expr
        };

        // Handle simple comparisons: column = value
        if let Some((left, right)) = self.split_comparison(expr, "=") {
            return self.evaluate_equals(left.trim(), right.trim());
        }

        // Handle inequalities  
        if let Some((left, right)) = self.split_comparison(expr, "!=") {
            let equals = self.evaluate_equals(left.trim(), right.trim())?;
            return Ok(!equals);
        }

        // Handle SESSION_CONTEXT calls
        if expr.contains("SESSION_CONTEXT") {
            return self.evaluate_session_context_expression(expr);
        }

        // Handle logical operators (AND, OR)
        if expr.contains(" AND ") {
            return self.evaluate_logical_and(expr);
        }
        
        if expr.contains(" OR ") {
            return self.evaluate_logical_or(expr);
        }

        // Default: try to evaluate as boolean literal
        match expr.to_uppercase().as_str() {
            "TRUE" => Ok(true),
            "FALSE" => Ok(false),
            _ => Err(anyhow!("Cannot evaluate expression: {}", expr)),
        }
    }

    /// Split expression on comparison operator
    fn split_comparison<'a>(&self, expr: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
        if let Some(pos) = expr.find(op) {
            let left = &expr[..pos];
            let right = &expr[pos + op.len()..];
            Some((left, right))
        } else {
            None
        }
    }

    /// Evaluate equality comparison
    fn evaluate_equals(&self, left: &str, right: &str) -> Result<bool> {
        let left_value = self.resolve_value(left)?;
        let right_value = self.resolve_value(right)?;
        
        Ok(left_value == right_value)
    }

    /// Resolve a value (column reference, literal, or function call)
    fn resolve_value(&self, value: &str) -> Result<String> {
        let value = value.trim();

        // String literal
        if (value.starts_with('\'') && value.ends_with('\'')) ||
           (value.starts_with('"') && value.ends_with('"')) {
            return Ok(value[1..value.len()-1].to_string());
        }

        // SESSION_CONTEXT function
        if value.starts_with("SESSION_CONTEXT(") && value.ends_with(")") {
            let key = &value[16..value.len()-1]; // Remove "SESSION_CONTEXT(" and ")"
            let key = key.trim_matches('\'').trim_matches('"'); // Remove quotes
            return self.get_session_context(key);
        }

        // Column reference - check row data
        if let Some(row_value) = self.row_data.get(value) {
            return Ok(row_value.clone());
        }

        // Numeric literal
        if value.parse::<f64>().is_ok() {
            return Ok(value.to_string());
        }

        // Unknown - return as is
        Ok(value.to_string())
    }

    /// Get session context value
    fn get_session_context(&self, key: &str) -> Result<String> {
        self.session_context
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow!("Session context key '{}' not found", key))
    }

    /// Evaluate SESSION_CONTEXT expression
    fn evaluate_session_context_expression(&self, expr: &str) -> Result<bool> {
        // This handles expressions like: region = SESSION_CONTEXT('user_region')
        if let Some((left, right)) = self.split_comparison(expr, "=") {
            return self.evaluate_equals(left.trim(), right.trim());
        }
        
        Err(anyhow!("Cannot evaluate SESSION_CONTEXT expression: {}", expr))
    }

    /// Evaluate logical AND
    fn evaluate_logical_and(&self, expr: &str) -> Result<bool> {
        let parts: Vec<&str> = expr.split(" AND ").collect();
        
        for part in parts {
            if !self.evaluate_expression(part.trim())? {
                return Ok(false);
            }
        }
        
        Ok(true)
    }

    /// Evaluate logical OR  
    fn evaluate_logical_or(&self, expr: &str) -> Result<bool> {
        let parts: Vec<&str> = expr.split(" OR ").collect();
        
        for part in parts {
            if self.evaluate_expression(part.trim())? {
                return Ok(true);
            }
        }
        
        Ok(false)
    }
}

impl Default for ExpressionEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to create sample row data for testing
pub fn create_sample_row(data: Vec<(&str, &str)>) -> HashMap<String, String> {
    data.into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Helper to create session context
pub fn create_session_context(data: Vec<(&str, &str)>) -> HashMap<String, String> {
    data.into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_equality() {
        let mut evaluator = ExpressionEvaluator::new();
        
        // Set up row data
        evaluator.set_row_data(create_sample_row(vec![
            ("region", "west"),
            ("department", "sales"),
        ]));

        let filter = RowFilter {
            expression: "region = 'west'".to_string(),
            session_context: None,
        };

        let result = evaluator.evaluate_filter(&filter).unwrap();
        assert!(result);
    }

    #[test]
    fn test_session_context() {
        let mut evaluator = ExpressionEvaluator::new();
        
        // Set up session context  
        evaluator.set_session_context(create_session_context(vec![
            ("user_region", "west"),
            ("user_department", "engineering"),
        ]));
        
        // Set up row data
        evaluator.set_row_data(create_sample_row(vec![
            ("region", "west"),
            ("department", "engineering"),
        ]));

        let filter = RowFilter {
            expression: "region = SESSION_CONTEXT('user_region')".to_string(),
            session_context: None,
        };

        let result = evaluator.evaluate_filter(&filter).unwrap();
        assert!(result);
    }

    #[test]
    fn test_logical_and() {
        let mut evaluator = ExpressionEvaluator::new();
        
        evaluator.set_session_context(create_session_context(vec![
            ("user_region", "west"),
            ("user_department", "engineering"),
        ]));
        
        evaluator.set_row_data(create_sample_row(vec![
            ("region", "west"),
            ("department", "engineering"),
        ]));

        let filter = RowFilter {
            expression: "region = SESSION_CONTEXT('user_region') AND department = SESSION_CONTEXT('user_department')".to_string(),
            session_context: None,
        };

        let result = evaluator.evaluate_filter(&filter).unwrap();
        assert!(result);
    }

    #[test]
    fn test_access_denied() {
        let mut evaluator = ExpressionEvaluator::new();
        
        evaluator.set_session_context(create_session_context(vec![
            ("user_region", "east"), // User is from east
        ]));
        
        evaluator.set_row_data(create_sample_row(vec![
            ("region", "west"), // But row is from west
        ]));

        let filter = RowFilter {
            expression: "region = SESSION_CONTEXT('user_region')".to_string(),
            session_context: None,
        };

        let result = evaluator.evaluate_filter(&filter).unwrap();
        assert!(!result); // Should be denied
    }

    #[test]
    fn test_inequality() {
        let mut evaluator = ExpressionEvaluator::new();
        
        evaluator.set_row_data(create_sample_row(vec![
            ("status", "active"),
        ]));

        let filter = RowFilter {
            expression: "status != 'inactive'".to_string(),
            session_context: None,
        };

        let result = evaluator.evaluate_filter(&filter).unwrap();
        assert!(result);
    }
}