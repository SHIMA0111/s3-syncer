# s3-syncer

`s3-syncer` is a high-performance CLI tool designed for migrating data between AWS S3 buckets. It supports cross-account copying using AWS profiles and is built with a Hexagonal Architecture for extensibility.

## Features

- **Cross-Account Support**: seamless copying between buckets in different AWS accounts using independent source and destination profiles.
- **High Performance**: Concurrent file copying using a worker pool to maximize throughput.
- **Large File Support**: Automatically handles large files using multipart uploads via the AWS SDK Manager.
- **Progress Tracking**: Real-time progress bar displaying files found and copied.
- **Selective Copying**: Filter objects by prefix.

## Installation

### From Source

Requirements: Go 1.18 or later.

```bash
git clone https://github.com/SHIMA0111/s3-syncer.git
cd s3-syncer
go build -o s3-syncer ./cmd
```

## Usage

### Basic Usage

Copy from one bucket to another in the default account:

```bash
./s3-syncer --src-bucket my-source-bucket --dst-bucket my-dest-bucket
```

### Cross-Account Copy

To copy between accounts, ensure you have profiles configured in your `~/.aws/credentials` or `~/.aws/config`.

```bash
./s3-syncer \
  --src-profile profile-a \
  --dst-profile profile-b \
  --src-bucket source-bucket \
  --dst-bucket dest-bucket \
  --region us-west-2
```

### Concurrency Tuning Guide

You can adjust the `--workers` flag. While Goroutines are efficient and can handle thousands of concurrent tasks, the bottleneck is usually **AWS S3 API Rate Limits** or network bandwidth.

#### Recommended Settings

| Environment Spec                       | Workers     | Note                               |
|----------------------------------------|-------------|------------------------------------|
| **Local PC / Low Bandwidth**           | 10 - 50     | Default (10) is safe.              |
| **Standard Server (2-4 vCPU)**         | 50 - 200    | Good for most use cases.           |
| **High End Server (8+ vCPU, 10Gbps+)** | 300 - 1000+ | Effective for massive small files. |

#### Note on S3 API Limits
S3 supports **3,500 PUT/COPY/POST/DELETE** and **5,500 GET/HEAD** requests per second per prefix.
Exceeding this with too many workers (e.g., 2000+) may cause `503 Slow Down` errors, triggering backoff strategies and slowing down the migration.

```bash
# Example for high-spec machine
./s3-syncer --src-bucket source --dst-bucket dest --workers 300
```

### Flags

| Flag            | Description                           | Default   |
|-----------------|---------------------------------------|-----------|
| `--src-bucket`  | Source S3 bucket name (required)      |           |
| `--dst-bucket`  | Destination S3 bucket name (required) |           |
| `--src-profile` | AWS profile for source account        | default   |
| `--dst-profile` | AWS profile for destination account   | default   |
| `--prefix`      | Prefix filter for keys to copy        | ""        |
| `--region`      | AWS Region                            | us-east-1 |
| `--workers`     | Number of concurrent workers          | 10        |

## Architecture

This project follows the **Hexagonal Architecture (Ports and Adapters)** pattern:

- **Core (Domain)**: accessible in `internal/core`. Defines the business logic and interfaces (`ports`).
- **Adapters**: accessible in `internal/adapters`. Contains the implementation for S3 (`adapters/s3`) and the CLI (`adapters/cli`).

This design allows for easy testing and future extensions (e.g., adding support for Local Filesystem, Google Cloud Storage, or Azure Blob Storage) by simply implementing the `Storage` interface.

# License
This project is under the Apache 2.0 license.
Please refer to the [LICENSE](LICENSE.md) file for details.