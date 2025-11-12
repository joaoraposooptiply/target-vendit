# target-vendit

A Singer target for sending data to the Vendit API. This target processes buy orders and converts them into pre-purchase orders for the Vendit system.

## Features

- **Preprocessing Support**: Automatically converts buy orders with line items into individual pre-purchase orders
- **Flexible Data Handling**: Supports multiple data formats and field mappings
- **Batch Processing**: Efficiently processes multiple records in batches
- **Production Ready**: Configured for production Vendit API endpoints

## Installation

```bash
pip install target-vendit
```

## Configuration

The target requires the following configuration:

```json
{
  "vendit_api_key": "your_api_key",
  "username": "your_username", 
  "password": "your_password",
  "api_url": "https://api2.vendit.online",
  "batch_size": 100
}
```

### Configuration Options

- `vendit_api_key` (required): Your Vendit API key
- `username` (required): Your Vendit username
- `password` (required): Your Vendit password  
- `api_url` (optional): Vendit API URL (defaults to https://api2.vendit.online)
- `batch_size` (optional): Number of records to process in each batch (default: 100)

## Usage

### Basic Usage

```bash
target-vendit --config config.json < input.jsonl
```

### With Singer Tap

```bash
tap-source --config tap_config.json | target-vendit --config target_config.json
```

## Data Processing

The target automatically handles different data structures:

### Buy Orders with Line Items

Input buy orders with line items are automatically expanded into individual pre-purchase orders:

```json
{
  "id": 12345,
  "transaction_date": "2025-01-15T10:00:00Z",
  "line_items": "[{\"product_remoteId\": 240, \"quantity\": 5, \"sku\": \"PROD-001\"}]"
}
```

Becomes:

```json
{
  "items": [
    {
      "productId": 240,
      "amount": 5,
      "optiplyId": "12345",
      "creationDatetime": "2025-01-15T10:00:00Z"
    }
  ]
}
```

### Direct Pre-Purchase Orders

Direct pre-purchase order records are processed as-is:

```json
{
  "productId": 240,
  "amount": 5,
  "optiplyId": "12345"
}
```

## Field Mapping

The target automatically maps common field names:

- `id` or `buyOrderId` → `optiplyId`
- `transaction_date` or `placedDate` or `created_at` → `creationDatetime`
- `product_remoteId` → `productId`
- `quantity` → `amount`

## Development

### Setup

```bash
git clone <repository>
cd target-vendit
pip install -e .
```

### Code Quality

```bash
# Format code
black target_vendit/

# Sort imports
isort target_vendit/

# Type checking
mypy target_vendit/

# Linting
flake8 target_vendit/
```

## License

Apache License 2.0