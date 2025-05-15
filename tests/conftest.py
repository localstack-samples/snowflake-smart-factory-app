import pytest
import os
import logging
import snowflake.connector

# Configure logging for test runs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    """Set up environment variables for testing if not already set"""
    env_vars = {
        "SNOWFLAKE_USER": "test",
        "SNOWFLAKE_PASSWORD": "test",
        "SNOWFLAKE_ACCOUNT": "test",
        "SNOWFLAKE_WAREHOUSE": "test",
        "SNOWFLAKE_HOST": "snowflake.localhost.localstack.cloud",
        "SNOWFLAKE_PORT": "4566",
    }
    
    # Only set variables that aren't already defined
    for key, value in env_vars.items():
        if not os.getenv(key):
            os.environ[key] = value
    
    logger.info("Test environment configured")
    yield
    logger.info("Test environment cleanup complete")

@pytest.fixture
def snowflake_conn():
    """Fixture to provide a Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER", "test"),
            password=os.getenv("SNOWFLAKE_PASSWORD", "test"),
            account=os.getenv("SNOWFLAKE_ACCOUNT", "test"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "test"),
            database="FACTORY_PIPELINE_DEMO",
            schema="PUBLIC",
            host=os.getenv("SNOWFLAKE_HOST", "snowflake.localhost.localstack.cloud"),
            port=os.getenv("SNOWFLAKE_PORT", "4566"),
        )
        logger.info("Successfully connected to Snowflake")
        yield conn
        conn.close()
        logger.info("Closed Snowflake connection")
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        pytest.fail(f"Failed to connect to Snowflake: {e}")

# Helper function for standalone script mode
def get_snowflake_connection():
    """Get a Snowflake connection for standalone script mode"""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER", "test"),
        password=os.getenv("SNOWFLAKE_PASSWORD", "test"),
        account=os.getenv("SNOWFLAKE_ACCOUNT", "test"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "test"),
        database="FACTORY_PIPELINE_DEMO",
        schema="PUBLIC",
        host=os.getenv("SNOWFLAKE_HOST", "snowflake.localhost.localstack.cloud"),
        port=os.getenv("SNOWFLAKE_PORT", "4566"),
    ) 