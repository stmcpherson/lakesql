# LakeSQL

A Lake Formation DDL client with SQL-like syntax for managing AWS data lake permissions and resources.

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## 🚀 Features

- **SQL-like DDL Syntax**: Natural language for Lake Formation operations
- **Multi-Backend Support**: AWS Lake Formation + Local Emulator
- **Tag-Based Access Control**: Advanced TBAC permissions
- **Column-Level Security**: Fine-grained data access control
- **Cross-Account Permissions**: Multi-account data sharing
- **WASM Bindings**: Browser-based usage
- **CLI Tool**: Command-line interface for automation
- **Local Development**: Test without AWS costs

## 📝 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/stmcpherson/lakesql.git
cd lakesql

# Build all crates
cargo build --release

# Install CLI tool
cargo install --path crates/lakesql-cli
```

### Basic Usage

```bash
# Start local emulator
lakesql-cli emulator start

# Run DDL commands
lakesql-cli exec "GRANT SELECT ON sales.orders TO ROLE data_scientist"
lakesql-cli exec "CREATE ROLE analytics_team" 
lakesql-cli exec "CREATE TAG department VALUES ('finance', 'marketing', 'engineering')"
```

### Configuration

Create `~/.lakesql/config.toml`:

```toml
[aws]
region = "us-east-1"
profile = "default"

[emulator]
port = 8080
storage_path = "~/.lakesql/data"

[logging]
level = "info"
```

## 🏗️ Architecture

LakeSQL is built as a multi-crate workspace:

```
lakesql/
├── crates/
│   ├── lakesql-core/      # Core types and traits
│   ├── lakesql-parser/    # SQL DDL parser
│   ├── lakesql-emulator/  # Local development emulator  
│   ├── lakesql-aws/       # AWS Lake Formation integration
│   ├── lakesql-wasm/      # WebAssembly bindings
│   └── lakesql-cli/       # Command-line interface
├── demo.rs                # Usage examples
└── demo_test.rs          # Integration tests
```

### Core Components

#### 🎯 **Core Types** (`lakesql-core`)

- **Principal**: Users, Roles, SAML Groups, Tag-based principals
- **Resource**: Databases, Tables, Data Locations, Tagged resources  
- **Action**: SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP
- **Permission**: Complete permission model with row filters

#### 📝 **Parser** (`lakesql-parser`)

Supports standard DDL syntax:

```sql
-- Grant permissions
GRANT SELECT, INSERT ON sales.orders TO ROLE data_scientist;
GRANT ALL ON DATABASE analytics TO USER 'alice@company.com';

-- Create resources
CREATE ROLE analytics_team;
CREATE TAG environment VALUES ('prod', 'staging', 'dev');

-- Revoke permissions  
REVOKE DELETE ON sales.customers FROM ROLE intern;

-- Tag-based access
GRANT SELECT ON TAGGED RESOURCE 
WHERE environment = 'prod' AND department = 'finance'
TO TAGGED PRINCIPAL WHERE team = 'analysts';
```

#### ☁️ **AWS Integration** (`lakesql-aws`)

- Real Lake Formation API calls
- Cross-account permission management
- IAM integration
- CloudTrail logging support

#### 🔧 **Emulator** (`lakesql-emulator`)

- Local development environment
- No AWS costs during testing
- Persistent storage with sled
- Compatible API surface

## 🛠️ Development

### Prerequisites

- Rust 1.70+ 
- AWS CLI configured (for AWS backend)
- Node.js 18+ (for WASM development)

### Setup Development Environment

```bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install required targets
rustup target add wasm32-unknown-unknown

# Install wasm-pack for WASM builds
cargo install wasm-pack

# Run tests
cargo test --workspace

# Run specific crate tests
cargo test -p lakesql-core
cargo test -p lakesql-parser

# Build WASM package
cd crates/lakesql-wasm
wasm-pack build --target web
```

### Running Examples

```bash
# Run the demo
cargo run --bin demo

# Test DDL parsing
echo "GRANT SELECT ON sales.orders TO ROLE analyst" | cargo run --bin lakesql-cli -- parse

# Start emulator
cargo run --bin lakesql-cli -- emulator start --port 8080
```

## 📖 Examples

### Basic Permission Management

```rust
use lakesql_core::*;
use lakesql_parser::parse_ddl;

// Parse DDL statement
let sql = "GRANT SELECT ON sales.orders TO ROLE data_scientist";
let statement = parse_ddl(sql)?;

// Execute with AWS backend
let backend = AwsBackend::new().await?;
backend.execute(statement).await?;
```

### Tag-Based Access Control

```sql
-- Create tags
CREATE TAG department VALUES ('finance', 'marketing', 'hr');
CREATE TAG classification VALUES ('public', 'internal', 'confidential');

-- Tag resources
ALTER TABLE sales.orders SET TAGS (department='finance', classification='internal');

-- Grant tag-based permissions
GRANT SELECT ON TAGGED RESOURCE 
WHERE department = 'finance' AND classification IN ('public', 'internal')
TO TAGGED PRINCIPAL 
WHERE team = 'analysts';
```

### Column-Level Security

```sql
-- Grant access to specific columns
GRANT SELECT (customer_id, order_date, total) ON sales.orders TO ROLE junior_analyst;

-- Grant full table access except sensitive columns  
GRANT SELECT ON sales.orders EXCEPT (ssn, credit_card) TO ROLE contractor;
```

### Cross-Account Sharing

```sql
-- Share database with external account
GRANT SELECT ON DATABASE analytics 
TO EXTERNAL ACCOUNT '123456789012' 
WITH GRANT OPTION;

-- Create cross-account role
CREATE EXTERNAL ROLE 'arn:aws:iam::123456789012:role/DataAnalyst';
```

## 🌐 WASM Usage

```html
<!DOCTYPE html>
<html>
<head>
    <script type="module">
        import init, { parse_ddl, execute_local } from './pkg/lakesql_wasm.js';
        
        async function main() {
            await init();
            
            const sql = "GRANT SELECT ON sales.orders TO ROLE analyst";
            const parsed = parse_ddl(sql);
            console.log('Parsed:', parsed);
            
            const result = execute_local(parsed);
            console.log('Result:', result);
        }
        
        main();
    </script>
</head>
<body>
    <h1>LakeSQL WASM Demo</h1>
</body>
</html>
```

## 🧪 Testing

```bash
# Run all tests
cargo test --workspace

# Test with AWS integration (requires AWS credentials)
cargo test --workspace --features aws-integration

# Test WASM bindings
cd crates/lakesql-wasm
wasm-pack test --node

# Benchmark performance
cargo bench
```

## 🐳 Docker

```dockerfile
FROM rust:1.70-slim as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/lakesql-cli /usr/local/bin/
CMD ["lakesql-cli"]
```

```bash
# Build and run
docker build -t lakesql .
docker run -it lakesql lakesql-cli --help
```

## ⚡ Performance

- **Parser**: ~1ms for typical DDL statements
- **Memory**: <10MB typical usage  
- **Emulator**: 1000+ ops/sec on modest hardware
- **WASM**: <100KB gzipped bundle size

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow Rust naming conventions
- Add tests for new features
- Update documentation
- Run `cargo fmt` and `cargo clippy`
- Update CHANGELOG.md

## 🗺️ Roadmap

- [ ] **v0.2.0**: Advanced row-level security
- [ ] **v0.3.0**: GraphQL query interface  
- [ ] **v0.4.0**: Terraform provider integration
- [ ] **v0.5.0**: Visual permission editor (web UI)
- [ ] **v1.0.0**: Production-ready release

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- AWS Lake Formation team for the excellent service
- Rust community for amazing crates
- Contributors and early adopters

## 📞 Support

- 📚 **Documentation**: Check the `/docs` directory
- 🐛 **Issues**: Report bugs on GitHub Issues
- 💬 **Discussions**: Join GitHub Discussions

---

**Built with ❤️by the LunarLabs team**