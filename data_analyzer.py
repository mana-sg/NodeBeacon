import mysql.connector
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple
from tabulate import tabulate
import os
import dotenv
dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MySQL Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": "health_monitor"
}

def get_connection():
    """Get MySQL connection"""
    return mysql.connector.connect(**MYSQL_CONFIG)

def analyze_node_status_distribution(time_window_minutes: int = 60) -> Dict:
    """Analyze the distribution of node statuses in the last X minutes"""
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = """
            SELECT 
                status,
                COUNT(*) as count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
            FROM node_health_data
            WHERE timestamp >= NOW() - INTERVAL %s MINUTE
            GROUP BY status
            ORDER BY count DESC
        """
        
        cursor.execute(query, [time_window_minutes])
        results = cursor.fetchall()
        
        print(f"\n=== Node Status Distribution (Last {time_window_minutes} minutes) ===")
        headers = ["Status", "Count", "Percentage"]
        table_data = [[r['status'], r['count'], f"{r['percentage']:.2f}%"] for r in results]
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        return results
        
    except Exception as e:
        logger.error(f"Error analyzing status distribution: {e}")
        raise
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def analyze_node_failure_rates() -> Dict[int, float]:
    """Calculate failure rates for each node"""
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = """
            SELECT 
                node_id,
                COUNT(*) as total_events,
                SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) as failures,
                (SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as failure_rate,
                MAX(CASE WHEN status = 'Failed' THEN timestamp END) as last_failure
            FROM node_health_data
            GROUP BY node_id
            ORDER BY failure_rate DESC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\n=== Node Failure Analysis ===")
        headers = ["Node ID", "Total Events", "Failures", "Failure Rate", "Last Failure"]
        table_data = [
            [
                r['node_id'],
                r['total_events'],
                r['failures'],
                f"{r['failure_rate']:.2f}%",
                r['last_failure'] if r['last_failure'] else 'N/A'
            ] for r in results
        ]
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        return results
        
    except Exception as e:
        logger.error(f"Error analyzing failure rates: {e}")
        raise
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def analyze_recent_events(minutes: int = 15) -> List[Dict]:
    """Analyze recent events in the last X minutes"""
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = """
            SELECT 
                node_id,
                status,
                timestamp,
                received_at,
                TIMESTAMPDIFF(SECOND, timestamp, received_at) as processing_delay
            FROM node_health_data
            WHERE timestamp >= NOW() - INTERVAL %s MINUTE
            ORDER BY timestamp DESC
            LIMIT 10
        """
        
        cursor.execute(query, [minutes])
        results = cursor.fetchall()
        
        print(f"\n=== Recent Events (Last {minutes} minutes) ===")
        headers = ["Node ID", "Status", "Timestamp", "Processing Delay (s)"]
        table_data = [
            [
                r['node_id'],
                r['status'],
                r['timestamp'],
                r['processing_delay']
            ] for r in results
        ]
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        return results
        
    except Exception as e:
        logger.error(f"Error analyzing recent events: {e}")
        raise
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def analyze_performance_metrics() -> Dict:
    """Analyze system performance metrics"""
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        queries = {
            "events_per_minute": """
                SELECT COUNT(*) / TIMESTAMPDIFF(MINUTE, MIN(timestamp), MAX(timestamp)) as events_per_minute
                FROM node_health_data
                WHERE timestamp >= NOW() - INTERVAL 1 HOUR
            """,
            "processing_delays": """
                SELECT 
                    AVG(TIMESTAMPDIFF(SECOND, timestamp, received_at)) as avg_delay,
                    MAX(TIMESTAMPDIFF(SECOND, timestamp, received_at)) as max_delay,
                    MIN(TIMESTAMPDIFF(SECOND, timestamp, received_at)) as min_delay
                FROM node_health_data
                WHERE timestamp >= NOW() - INTERVAL 1 HOUR
            """
        }
        
        results = {}
        for metric, query in queries.items():
            cursor.execute(query)
            results[metric] = cursor.fetchone()
        
        print("\n=== Performance Metrics (Last Hour) ===")
        print(f"Average Events per Minute: {results['events_per_minute']['events_per_minute']:.2f}")
        print(f"Average Processing Delay: {results['processing_delays']['avg_delay']:.2f} seconds")
        print(f"Maximum Processing Delay: {results['processing_delays']['max_delay']:.2f} seconds")
        print(f"Minimum Processing Delay: {results['processing_delays']['min_delay']:.2f} seconds")
        
        return results
        
    except Exception as e:
        logger.error(f"Error analyzing performance metrics: {e}")
        raise
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def run_analysis():
    """Run all analyses and display results"""
    print("\n" + "="*50)
    print("Node Health Monitoring System Analysis")
    print("="*50)
    
    try:
        analyze_node_status_distribution()
        analyze_node_failure_rates()
        analyze_recent_events()
        analyze_performance_metrics()
        
        print("\nAnalysis completed successfully!")
        print("="*50)
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        print("\nError occurred during analysis. Check logs for details.")

def main():
    run_analysis()

if __name__ == "__main__":
    main()