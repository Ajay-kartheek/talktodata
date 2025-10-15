"""Streamlit UI for Text-to-SQL application."""

import json
import logging
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from src.text2sql.config import get_settings, setup_logging
from src.text2sql.text2sql import Text2SQL


# Setup logging
setup_logging("INFO")
logger = logging.getLogger(__name__)


def init_session_state():
    """Initialize Streamlit session state."""
    if 'app' not in st.session_state:
        st.session_state.app = None
    if 'schema_loaded' not in st.session_state:
        st.session_state.schema_loaded = False
    if 'loaded_tables' not in st.session_state:
        st.session_state.loaded_tables = []
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    if 'schema_path' not in st.session_state:
        st.session_state.schema_path = None


def generate_schema_from_dataframes(dfs: Dict[str, pd.DataFrame]) -> dict:
    """Auto-generate schema from uploaded DataFrames.

    Args:
        dfs: Dictionary of table_name -> DataFrame

    Returns:
        Schema dictionary
    """
    tables = []

    for table_name, df in dfs.items():
        columns = []

        for col_name, dtype in df.dtypes.items():
            # Map pandas dtypes to SQL types
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "DECIMAL"
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "TIMESTAMP"
            else:
                sql_type = "VARCHAR"

            columns.append({
                "name": col_name,
                "type": sql_type,
                "description": f"{col_name} column",
                "nullable": True,
                "primary_key": False
            })

        tables.append({
            "name": table_name,
            "description": f"{table_name} table",
            "columns": columns
        })

    return {
        "database_name": "uploaded_data",
        "description": "Auto-generated schema from uploaded files",
        "tables": tables
    }


def load_data_files(uploaded_files: List, app: Text2SQL) -> List[str]:
    """Load uploaded data files into DuckDB.

    Args:
        uploaded_files: List of uploaded files from Streamlit
        app: Text2SQL application instance

    Returns:
        List of loaded table names
    """
    loaded_tables = []
    dataframes = {}

    for uploaded_file in uploaded_files:
        try:
            # Determine file type
            file_ext = Path(uploaded_file.name).suffix.lower()
            table_name = Path(uploaded_file.name).stem

            # Save to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name

            # Determine file type and load
            if file_ext == '.csv':
                df = pd.read_csv(tmp_path)
                app.load_data(table_name, tmp_path, "csv")
            elif file_ext == '.json':
                df = pd.read_json(tmp_path)
                app.load_data(table_name, tmp_path, "json")
            elif file_ext == '.parquet':
                df = pd.read_parquet(tmp_path)
                app.load_data(table_name, tmp_path, "parquet")
            else:
                st.warning(f"Unsupported file type: {uploaded_file.name}")
                continue

            dataframes[table_name] = df
            loaded_tables.append(table_name)

            # Cleanup temp file
            Path(tmp_path).unlink()

        except Exception as e:
            st.error(f"Error loading {uploaded_file.name}: {e}")

    return loaded_tables, dataframes


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Text-to-SQL",
        page_icon="🔍",
        layout="wide"
    )

    init_session_state()

    # Title and description
    st.title("🔍 Text-to-SQL with AWS Bedrock")
    st.markdown("""
    Upload your data files (CSV, JSON, Parquet) and ask questions in natural language.
    The system will automatically generate and execute SQL queries.
    """)

    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")

        # AWS Configuration
        with st.expander("AWS Settings", expanded=False):
            aws_region = st.text_input("AWS Region", value="us-east-1")
            aws_access_key = st.text_input("AWS Access Key ID", type="password")
            aws_secret_key = st.text_input("AWS Secret Access Key", type="password")
            model_id = st.text_input(
                "Bedrock Model ID",
                value="us.anthropic.claude-3-5-sonnet-20240620-v1:0"
            )

        st.divider()

        # Data Upload Section
        st.header("📁 Data Upload")

        uploaded_files = st.file_uploader(
            "Upload Data Files",
            type=['csv', 'json', 'parquet'],
            accept_multiple_files=True,
            help="Upload CSV, JSON, or Parquet files. Table names will be derived from filenames."
        )

        # Schema options
        schema_option = st.radio(
            "Schema Definition",
            ["Auto-generate from data", "Upload schema JSON"],
            help="Auto-generate creates schema from uploaded files"
        )

        uploaded_schema = None
        if schema_option == "Upload schema JSON":
            uploaded_schema = st.file_uploader(
                "Upload Schema JSON",
                type=['json'],
                help="Upload a schema definition JSON file"
            )

        # Initialize button
        if st.button("🚀 Initialize System", type="primary"):
            if not uploaded_files:
                st.error("Please upload at least one data file")
            else:
                with st.spinner("Initializing system..."):
                    try:
                        # Get settings
                        settings = get_settings()

                        # Override with user inputs if provided
                        if aws_access_key:
                            settings.aws_access_key_id = aws_access_key
                        if aws_secret_key:
                            settings.aws_secret_access_key = aws_secret_key
                        if aws_region:
                            settings.aws_region = aws_region
                        if model_id:
                            settings.bedrock_model_id = model_id

                        # Create temp schema file
                        with tempfile.NamedTemporaryFile(
                            mode='w',
                            suffix='.json',
                            delete=False
                        ) as tmp_schema:
                            # Load or generate schema
                            if uploaded_schema:
                                schema_data = json.load(uploaded_schema)
                                json.dump(schema_data, tmp_schema)
                            else:
                                # We'll generate after loading data
                                json.dump({
                                    "database_name": "temp",
                                    "tables": []
                                }, tmp_schema)

                            schema_path = tmp_schema.name

                        # Initialize app
                        st.session_state.app = Text2SQL(
                            schema_path=schema_path,
                            settings=settings
                        )

                        # Load data files
                        loaded_tables, dataframes = load_data_files(
                            uploaded_files,
                            st.session_state.app
                        )

                        # Generate schema if auto-generate selected
                        if schema_option == "Auto-generate from data":
                            schema_data = generate_schema_from_dataframes(dataframes)

                            # Update schema
                            with open(schema_path, 'w') as f:
                                json.dump(schema_data, f, indent=2)

                            # Reload schema
                            st.session_state.app.schema_loader.load_from_file(schema_path)

                        st.session_state.loaded_tables = loaded_tables
                        st.session_state.schema_loaded = True
                        st.session_state.schema_path = schema_path

                        st.success(f"✅ System initialized! Loaded {len(loaded_tables)} tables")
                        st.rerun()

                    except Exception as e:
                        st.error(f"Initialization failed: {e}")
                        logger.error(f"Initialization error: {e}", exc_info=True)

        # Show loaded tables
        if st.session_state.loaded_tables:
            st.divider()
            st.subheader("📊 Loaded Tables")
            for table in st.session_state.loaded_tables:
                st.text(f"✓ {table}")

    # Main content area
    if not st.session_state.schema_loaded:
        st.info("👈 Please upload your data files and initialize the system using the sidebar.")

        # Show example
        with st.expander("📖 Example Schema Format"):
            example_schema = {
                "database_name": "example",
                "description": "Example database",
                "tables": [
                    {
                        "name": "customers",
                        "description": "Customer information",
                        "columns": [
                            {
                                "name": "customer_id",
                                "type": "INTEGER",
                                "description": "Unique customer ID",
                                "nullable": False,
                                "primary_key": True
                            },
                            {
                                "name": "name",
                                "type": "VARCHAR",
                                "description": "Customer name",
                                "nullable": False
                            }
                        ]
                    }
                ]
            }
            st.json(example_schema)

    else:
        # Create tabs
        tab1, tab2, tab3 = st.tabs(["💬 Ask Questions", "📋 Schema Info", "📜 Query History"])

        with tab1:
            st.header("Ask Questions")

            # Question input
            question = st.text_area(
                "Enter your question in natural language:",
                height=100,
                placeholder="e.g., What are the top 5 customers by total order value?"
            )

            col1, col2 = st.columns([1, 4])
            with col1:
                execute_button = st.button("🔍 Generate & Execute", type="primary")
            with col2:
                validate_checkbox = st.checkbox("Validate SQL before execution", value=True)

            if execute_button and question:
                with st.spinner("Generating SQL query..."):
                    try:
                        # Execute query
                        results = st.session_state.app.query(
                            question=question,
                            validate=validate_checkbox
                        )

                        # Store in history
                        st.session_state.query_history.append(results)

                        if results['success']:
                            # Show SQL
                            st.subheader("📝 Generated SQL")
                            st.code(results['sql'], language='sql')

                            # Show metrics
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Rows Returned", results['row_count'])
                            with col2:
                                st.metric(
                                    "Complexity",
                                    results['metrics']['complexity_level'].upper()
                                )
                            with col3:
                                st.metric(
                                    "Joins",
                                    results['metrics']['num_joins']
                                )

                            # Show results
                            st.subheader("📊 Results")
                            if results['results']:
                                df = pd.DataFrame(results['results'])
                                st.dataframe(df, width='stretch')

                                # Download button
                                csv = df.to_csv(index=False)
                                st.download_button(
                                    "⬇️ Download Results (CSV)",
                                    csv,
                                    "query_results.csv",
                                    "text/csv",
                                    key='download-csv'
                                )
                            else:
                                st.info("Query executed successfully but returned no results")

                            # # Explanation
                            # with st.expander("💡 Explain Query"):
                            #     if st.button("Generate Explanation"):
                            #         with st.spinner("Generating explanation..."):
                            #             explanation = st.session_state.app.explain_sql(
                            #                 results['sql']
                            #             )
                            #             st.write(explanation)

                        else:
                            st.error(f"❌ Query failed: {results['error']}")
                            if results.get('sql'):
                                st.code(results['sql'], language='sql')

                    except Exception as e:
                        st.error(f"Error: {e}")
                        logger.error(f"Query error: {e}", exc_info=True)

        with tab2:
            st.header("Database Schema")

            # Show schema
            schema_info = st.session_state.app.get_schema_info()
            st.markdown(schema_info)

            # Table details
            st.subheader("Table Details")
            selected_table = st.selectbox(
                "Select a table to view details:",
                st.session_state.loaded_tables
            )

            if selected_table:
                try:
                    table_info = st.session_state.app.get_table_info(selected_table)

                    st.write(f"**Description:** {table_info.get('description', 'N/A')}")

                    # Show columns
                    st.write("**Columns:**")
                    cols_df = pd.DataFrame(table_info['columns'])
                    st.dataframe(cols_df, width='stretch')

                    # Show sample data
                    st.write("**Sample Data:**")
                    sample_df = pd.DataFrame(table_info['sample_data'])
                    st.dataframe(sample_df, width='stretch')

                except Exception as e:
                    st.error(f"Error loading table info: {e}")

        with tab3:
            st.header("Query History")

            if st.session_state.query_history:
                for i, query in enumerate(reversed(st.session_state.query_history)):
                    with st.expander(
                        f"Query {len(st.session_state.query_history) - i}: {query['question'][:50]}..."
                    ):
                        st.write(f"**Question:** {query['question']}")
                        st.code(query.get('sql', 'N/A'), language='sql')

                        if query['success']:
                            st.success(f"✅ Success - {query['row_count']} rows returned")
                        else:
                            st.error(f"❌ Failed - {query.get('error', 'Unknown error')}")
            else:
                st.info("No queries executed yet")


if __name__ == "__main__":
    main()
