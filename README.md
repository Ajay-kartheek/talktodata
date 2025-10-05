# Text-to-SQL with AWS Bedrock

A production-grade Text-to-SQL system that converts natural language questions into SQL queries using AWS Bedrock (Claude) and DuckDB.

## Features

- 🤖 **Natural Language to SQL**: Convert questions to SQL using Claude AI
- 📊 **Multiple Data Formats**: Support for CSV, JSON, and Parquet files
- 🔍 **Query Validation**: Automatic SQL validation before execution
- 🎨 **Streamlit UI**: Interactive web interface for easy use
- 🔧 **CLI Support**: Command-line interface for automation
- 📝 **Auto Schema Generation**: Automatically generate schema from uploaded files
- 🛡️ **SQL Injection Protection**: Built-in safety checks
- 📈 **Query Analytics**: Complexity analysis and metrics

## Installation

1. **Clone the repository**
```bash
cd TalkToData
```

2. **Create virtual environment**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure AWS credentials**

Create a `.env` file in the project root:
```bash
cp .env.example .env
```

Edit `.env` with your AWS credentials:
```
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here

BEDROCK_MODEL_ID=anthropic.claude-3-5-sonnet-20241022-v2:0
BEDROCK_TEMPERATURE=0.0
BEDROCK_MAX_TOKENS=4000
```

## Usage

### Streamlit UI (Recommended)

1. **Start the Streamlit app**
```bash
streamlit run app.py
```

2. **Upload your data files**
   - Click "Browse files" in the sidebar
   - Upload CSV, JSON, or Parquet files
   - Each file becomes a table (filename = table name)

3. **Choose schema option**
   - **Auto-generate**: Automatically creates schema from your data
   - **Upload JSON**: Provide a custom schema definition

4. **Initialize the system**
   - Click "🚀 Initialize System"

5. **Ask questions**
   - Type your question in natural language
   - Click "🔍 Generate & Execute"
   - View SQL, results, and download data

### CLI Usage

**Single query mode:**
```bash
python main.py \
  --schema config/schema.json \
  --data customers:data/customers.csv:csv \
  --data orders:data/orders.csv:csv \
  --query "What are the top 5 customers by total order value?"
```

**Interactive mode:**
```bash
python main.py \
  --schema config/schema.json \
  --data customers:data/customers.csv:csv \
  --data orders:data/orders.csv:csv
```

**JSON output:**
```bash
python main.py \
  --schema config/schema.json \
  --data customers:data/customers.csv:csv \
  --query "Show all customers" \
  --format json
```

## Schema Format

If you want to provide a custom schema, create a JSON file:

```json
{
  "database_name": "my_database",
  "description": "My database description",
  "tables": [
    {
      "name": "customers",
      "description": "Customer information",
      "columns": [
        {
          "name": "customer_id",
          "type": "INTEGER",
          "description": "Unique customer identifier",
          "nullable": false,
          "primary_key": true
        },
        {
          "name": "name",
          "type": "VARCHAR",
          "description": "Customer name",
          "nullable": false
        },
        {
          "name": "email",
          "type": "VARCHAR",
          "description": "Customer email",
          "nullable": true
        }
      ]
    }
  ]
}
```

### Foreign Keys

To define relationships between tables:

```json
{
  "name": "customer_id",
  "type": "INTEGER",
  "foreign_key": "customers.customer_id"
}
```

## Example Questions

Once your data is loaded, try questions like:

- "How many customers do we have?"
- "What are the top 10 products by revenue?"
- "Show me monthly sales for the last 6 months"
- "Which customers have never placed an order?"
- "What is the average order value by country?"
- "List all products that are out of stock"

## Project Structure

```
TalkToData/
├── src/text2sql/
│   ├── __init__.py
│   ├── config.py              # Configuration management
│   ├── duckdb_manager.py      # DuckDB database operations
│   ├── schema_loader.py       # Schema loading and parsing
│   ├── bedrock_client.py      # AWS Bedrock client
│   ├── prompt_builder.py      # LLM prompt construction
│   ├── sql_generator.py       # SQL generation orchestration
│   ├── query_validator.py     # SQL validation and safety
│   └── text2sql.py           # Main application class
├── app.py                     # Streamlit UI
├── main.py                    # CLI interface
├── requirements.txt           # Python dependencies
├── .env.example              # Example environment variables
└── README.md                 # This file
```

## How It Works

1. **Data Loading**: Upload CSV/JSON/Parquet files → DuckDB tables
2. **Schema Generation**: Auto-detect or use custom schema
3. **Question Processing**: Your question → Prompt Builder
4. **SQL Generation**: Prompt → AWS Bedrock (Claude) → SQL query
5. **Validation**: Check SQL for safety and correctness
6. **Execution**: Run SQL on DuckDB → Results
7. **Display**: Show results in table format

## Configuration Options

### Environment Variables

- `AWS_REGION`: AWS region (default: us-east-1)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `BEDROCK_MODEL_ID`: Bedrock model ID
- `BEDROCK_TEMPERATURE`: Model temperature (0.0 = deterministic)
- `BEDROCK_MAX_TOKENS`: Maximum tokens to generate
- `DUCKDB_PATH`: Database path (:memory: for in-memory)
- `LOG_LEVEL`: Logging level (INFO, DEBUG, WARNING, ERROR)

### Validation Settings

- **SQL Injection Protection**: Blocks dangerous patterns
- **Schema Validation**: Ensures tables/columns exist
- **Syntax Validation**: Uses DuckDB EXPLAIN
- **Query Type Filtering**: Restrict to SELECT queries only

## Troubleshooting

### AWS Credentials Error
- Ensure `.env` file exists with correct credentials
- Or provide credentials in Streamlit sidebar
- Check IAM permissions for Bedrock access

### Import Errors
```bash
pip install -r requirements.txt
```

### DuckDB Errors
- Check data file format matches specified type
- Ensure column names don't have special characters
- Verify file paths are correct

### LLM Generation Issues
- Increase `BEDROCK_MAX_TOKENS` for complex queries
- Check schema is properly formatted
- Review prompt in logs

## Advanced Features

### Custom Instructions

Add custom instructions to queries:
```python
app.query(
    question="Show sales data",
    custom_instructions="Always include currency symbols"
)
```

### Query Explanation

Get natural language explanation of SQL:
```python
explanation = app.explain_sql("SELECT * FROM customers")
```

### Query Metrics

Analyze query complexity:
```python
results = app.query("Your question")
print(results['metrics'])
# Output: {
#   'num_joins': 2,
#   'complexity_level': 'medium',
#   'complexity_score': 5
# }
```

## License

MIT License

## Support

For issues and questions, please check the project repository.
