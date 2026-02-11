import logging
import os
import sys

def setup_logging():
    """
    Sets up structured logging for Google Cloud Platform.
    
    Uses google-cloud-logging's StructuredLogHandler when running in a Cloud Function
    environment (detected via K_SERVICE or FUNCTION_TARGET env vars).
    Falls back to standard text logging for local development.
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Heuristic to detect Cloud Run / Cloud Functions environment
    is_cloud_env = bool(os.getenv("K_SERVICE") or os.getenv("FUNCTION_TARGET"))

    if is_cloud_env:
        try:
            # Attempt to use the Google Cloud Logging library for JSON structured logs
            import google.cloud.logging
            from google.cloud.logging.handlers import StructuredLogHandler

            client = google.cloud.logging.Client()
            handler = StructuredLogHandler()
            
            # Configure the root logger
            root = logging.getLogger()
            root.setLevel(log_level)
            
            # Remove existing handlers to avoid duplication (e.g. from container runtime)
            for h in root.handlers[:]:
                root.removeHandler(h)
                
            root.addHandler(handler)
            return
            
        except ImportError:
            # Fallback if the library isn't installed
            print("Warning: google-cloud-logging not found. Using standard logging.", file=sys.stderr)

    # Standard logging for local dev or fallback
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
        stream=sys.stdout
    )
