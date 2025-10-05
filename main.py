"""CLI interface for Text-to-SQL system."""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

from src.text2sql.config import get_settings, setup_logging
from src.text2sql.text2sql import Text2SQL


def format_results(results: Dict[str, Any]) -> str:
    """Format query results for display.

    Args:
        results: Query results dictionary

    Returns:
        Formatted string
    """
    if not results["success"]:
        return f"❌ Error: {results['error']}"

    output = []
    output.append(f"\n{'='*80}")
    output.append(f"Question: {results['question']}")
    output.append(f"{'='*80}")
    output.append(f"\nGenerated SQL:\n{results['sql']}")
    output.append(f"\n{'='*80}")
    output.append(f"Results ({results['row_count']} rows):")
    output.append(f"{'='*80}\n")

    # Display results as formatted table
    if results['results']:
        # Get column names
        columns = list(results['results'][0].keys())

        # Calculate column widths
        widths = {col: len(str(col)) for col in columns}
        for row in results['results']:
            for col in columns:
                widths[col] = max(widths[col], len(str(row.get(col, ''))))

        # Print header
        header = " | ".join(str(col).ljust(widths[col]) for col in columns)
        output.append(header)
        output.append("-" * len(header))

        # Print rows
        for row in results['results']:
            row_str = " | ".join(str(row.get(col, '')).ljust(widths[col]) for col in columns)
            output.append(row_str)
    else:
        output.append("No results returned")

    output.append(f"\n{'='*80}")
    output.append(f"Query Complexity: {results['metrics']['complexity_level'].upper()}")
    output.append(f"Complexity Score: {results['metrics']['complexity_score']}")
    output.append(f"{'='*80}\n")

    return "\n".join(output)


def interactive_mode(app: Text2SQL) -> None:
    """Run interactive query mode.

    Args:
        app: Text2SQL application instance
    """
    print("\n" + "="*80)
    print("Text-to-SQL Interactive Mode")
    print("="*80)
    print("\nAvailable commands:")
    print("  - Type your question in natural language")
    print("  - 'tables' - List all tables")
    print("  - 'schema' - Show database schema")
    print("  - 'info <table>' - Show table information")
    print("  - 'explain <sql>' - Explain SQL query")
    print("  - 'quit' or 'exit' - Exit the application")
    print("="*80 + "\n")

    while True:
        try:
            user_input = input("Query> ").strip()

            if not user_input:
                continue

            if user_input.lower() in ['quit', 'exit']:
                print("Goodbye!")
                break

            elif user_input.lower() == 'tables':
                tables = app.list_tables()
                print(f"\nAvailable tables ({len(tables)}):")
                for table in tables:
                    print(f"  - {table}")
                print()

            elif user_input.lower() == 'schema':
                schema = app.get_schema_info()
                print(f"\n{schema}\n")

            elif user_input.lower().startswith('info '):
                table_name = user_input[5:].strip()
                try:
                    info = app.get_table_info(table_name)
                    print(f"\nTable: {info['name']}")
                    if info['description']:
                        print(f"Description: {info['description']}")
                    print(f"\nColumns:")
                    for col in info['columns']:
                        print(f"  - {col['name']} ({col['type']})", end='')
                        if col['description']:
                            print(f" - {col['description']}", end='')
                        print()
                    print(f"\nSample Data:")
                    for row in info['sample_data']:
                        print(f"  {row}")
                    print()
                except Exception as e:
                    print(f"Error: {e}\n")

            elif user_input.lower().startswith('explain '):
                sql = user_input[8:].strip()
                try:
                    explanation = app.explain_sql(sql)
                    print(f"\n{explanation}\n")
                except Exception as e:
                    print(f"Error: {e}\n")

            else:
                # Treat as natural language query
                results = app.query(user_input)
                print(format_results(results))

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}\n")


def single_query_mode(app: Text2SQL, question: str, output_format: str = "text") -> None:
    """Execute a single query and exit.

    Args:
        app: Text2SQL application instance
        question: Question to ask
        output_format: Output format (text or json)
    """
    results = app.query(question)

    if output_format == "json":
        # Convert to JSON-serializable format
        output = {
            "success": results["success"],
            "question": results["question"],
            "sql": results.get("sql"),
            "results": results.get("results", []),
            "row_count": results.get("row_count", 0),
            "error": results.get("error")
        }
        print(json.dumps(output, indent=2))
    else:
        print(format_results(results))


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Text-to-SQL: Convert natural language to SQL queries"
    )

    parser.add_argument(
        "--schema",
        required=True,
        help="Path to schema JSON file"
    )

    parser.add_argument(
        "--data",
        nargs='+',
        help="Data files to load (format: table_name:file_path:file_type)"
    )

    parser.add_argument(
        "--query",
        "-q",
        help="Single query to execute (non-interactive mode)"
    )

    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format for single query mode"
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Logging level"
    )

    args = parser.parse_args()

    # Setup logging
    settings = get_settings()
    log_level = args.log_level or settings.log_level
    setup_logging(log_level)

    # Validate schema file
    if not Path(args.schema).exists():
        print(f"Error: Schema file not found: {args.schema}")
        sys.exit(1)

    try:
        # Initialize application
        with Text2SQL(schema_path=args.schema, settings=settings) as app:

            # Load data files if provided
            if args.data:
                for data_spec in args.data:
                    try:
                        parts = data_spec.split(':')
                        if len(parts) != 3:
                            print(f"Warning: Invalid data spec '{data_spec}'. Expected format: table:path:type")
                            continue

                        table_name, file_path, file_type = parts
                        app.load_data(table_name, file_path, file_type)
                        print(f"Loaded {table_name} from {file_path}")

                    except Exception as e:
                        print(f"Error loading data: {e}")
                        sys.exit(1)

            # Run in appropriate mode
            if args.query:
                single_query_mode(app, args.query, args.format)
            else:
                interactive_mode(app)

    except Exception as e:
        logging.error(f"Application error: {e}", exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
