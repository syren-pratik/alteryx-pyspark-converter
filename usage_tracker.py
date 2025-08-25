"""
Usage Tracking System for Alteryx to PySpark Converter
Tracks token usage, API calls, and provides analytics
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import sqlite3
from pathlib import Path

class UsageTracker:
    def __init__(self, db_path: str = "usage_data.db"):
        """Initialize the usage tracker with SQLite database"""
        self.db_path = db_path
        self._init_database()
        
        # Token pricing (per 1K tokens)
        self.pricing = {
            'gemini': {
                'input': 0.000125,  # Gemini 1.5 Flash pricing
                'output': 0.000375
            },
            'openai': {
                'gpt-4': {'input': 0.03, 'output': 0.06},
                'gpt-4-turbo': {'input': 0.01, 'output': 0.03},
                'gpt-3.5-turbo': {'input': 0.0005, 'output': 0.0015}
            },
            'ollama': {
                'input': 0,  # Free for local
                'output': 0
            }
        }
    
    def _init_database(self):
        """Initialize SQLite database with usage tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create usage logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS usage_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                user_ip TEXT,
                file_name TEXT,
                file_size INTEGER,
                workflow_name TEXT,
                total_tools INTEGER,
                provider TEXT,
                model TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                total_tokens INTEGER,
                cost_usd REAL,
                response_time_ms INTEGER,
                success BOOLEAN,
                error_message TEXT,
                improvements_count INTEGER,
                accuracy_score INTEGER
            )
        ''')
        
        # Create daily summary table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_summary (
                date DATE PRIMARY KEY,
                total_requests INTEGER DEFAULT 0,
                successful_requests INTEGER DEFAULT 0,
                failed_requests INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                total_cost_usd REAL DEFAULT 0,
                unique_users INTEGER DEFAULT 0,
                avg_accuracy_score REAL,
                most_used_provider TEXT
            )
        ''')
        
        # Create provider stats table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS provider_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE,
                provider TEXT,
                model TEXT,
                request_count INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                total_cost_usd REAL DEFAULT 0,
                avg_response_time_ms INTEGER,
                UNIQUE(date, provider, model)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def estimate_tokens(self, text: str) -> int:
        """Estimate token count (rough approximation)"""
        # Rough estimation: 1 token ≈ 4 characters for English text
        # This is a simplified estimate; actual tokenization varies
        return len(text) // 4
    
    def calculate_cost(self, provider: str, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost based on provider and token counts"""
        if provider == 'gemini':
            input_cost = (input_tokens / 1000) * self.pricing['gemini']['input']
            output_cost = (output_tokens / 1000) * self.pricing['gemini']['output']
        elif provider == 'openai':
            model_pricing = self.pricing['openai'].get(model, self.pricing['openai']['gpt-4'])
            input_cost = (input_tokens / 1000) * model_pricing['input']
            output_cost = (output_tokens / 1000) * model_pricing['output']
        else:  # ollama or local
            input_cost = 0
            output_cost = 0
        
        return round(input_cost + output_cost, 6)
    
    def log_usage(self, 
                  user_ip: str,
                  file_name: str,
                  file_size: int,
                  workflow_name: str,
                  total_tools: int,
                  provider: str,
                  model: str,
                  input_text: str,
                  output_text: str,
                  response_time_ms: int,
                  success: bool,
                  error_message: Optional[str] = None,
                  improvements_count: int = 0,
                  accuracy_score: int = 0) -> Dict:
        """Log a usage event to the database"""
        
        # Estimate tokens
        input_tokens = self.estimate_tokens(input_text)
        output_tokens = self.estimate_tokens(output_text)
        total_tokens = input_tokens + output_tokens
        
        # Calculate cost
        cost_usd = self.calculate_cost(provider, model, input_tokens, output_tokens)
        
        # Insert into database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO usage_logs (
                user_ip, file_name, file_size, workflow_name, total_tools,
                provider, model, input_tokens, output_tokens, total_tokens,
                cost_usd, response_time_ms, success, error_message,
                improvements_count, accuracy_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_ip, file_name, file_size, workflow_name, total_tools,
            provider, model, input_tokens, output_tokens, total_tokens,
            cost_usd, response_time_ms, success, error_message,
            improvements_count, accuracy_score
        ))
        
        log_id = cursor.lastrowid
        
        # Update daily summary
        today = datetime.now().date()
        cursor.execute('''
            INSERT OR REPLACE INTO daily_summary (
                date, total_requests, successful_requests, failed_requests,
                total_tokens, total_cost_usd
            ) VALUES (
                ?,
                COALESCE((SELECT total_requests FROM daily_summary WHERE date = ?), 0) + 1,
                COALESCE((SELECT successful_requests FROM daily_summary WHERE date = ?), 0) + ?,
                COALESCE((SELECT failed_requests FROM daily_summary WHERE date = ?), 0) + ?,
                COALESCE((SELECT total_tokens FROM daily_summary WHERE date = ?), 0) + ?,
                COALESCE((SELECT total_cost_usd FROM daily_summary WHERE date = ?), 0) + ?
            )
        ''', (
            today, today, today, 1 if success else 0,
            today, 0 if success else 1,
            today, total_tokens,
            today, cost_usd
        ))
        
        # Update provider stats
        cursor.execute('''
            INSERT OR REPLACE INTO provider_stats (
                date, provider, model, request_count, total_tokens, total_cost_usd, avg_response_time_ms
            ) VALUES (
                ?, ?, ?,
                COALESCE((SELECT request_count FROM provider_stats WHERE date = ? AND provider = ? AND model = ?), 0) + 1,
                COALESCE((SELECT total_tokens FROM provider_stats WHERE date = ? AND provider = ? AND model = ?), 0) + ?,
                COALESCE((SELECT total_cost_usd FROM provider_stats WHERE date = ? AND provider = ? AND model = ?), 0) + ?,
                ?
            )
        ''', (
            today, provider, model,
            today, provider, model,
            today, provider, model, total_tokens,
            today, provider, model, cost_usd,
            response_time_ms
        ))
        
        conn.commit()
        conn.close()
        
        return {
            'log_id': log_id,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'total_tokens': total_tokens,
            'cost_usd': cost_usd,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_usage_logs(self, 
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None,
                       provider: Optional[str] = None,
                       limit: int = 100) -> List[Dict]:
        """Get usage logs with optional filtering"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = "SELECT * FROM usage_logs WHERE 1=1"
        params = []
        
        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
        
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())
        
        if provider:
            query += " AND provider = ?"
            params.append(provider)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_daily_summary(self, days: int = 30) -> List[Dict]:
        """Get daily summary for the last N days"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        start_date = (datetime.now() - timedelta(days=days)).date()
        
        cursor.execute('''
            SELECT * FROM daily_summary 
            WHERE date >= ? 
            ORDER BY date DESC
        ''', (start_date,))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_provider_stats(self, days: int = 30) -> Dict[str, Any]:
        """Get provider statistics for the last N days"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        start_date = (datetime.now() - timedelta(days=days)).date()
        
        # Get provider totals
        cursor.execute('''
            SELECT 
                provider,
                model,
                SUM(request_count) as total_requests,
                SUM(total_tokens) as total_tokens,
                SUM(total_cost_usd) as total_cost,
                AVG(avg_response_time_ms) as avg_response_time
            FROM provider_stats
            WHERE date >= ?
            GROUP BY provider, model
            ORDER BY total_requests DESC
        ''', (start_date,))
        
        provider_data = [dict(row) for row in cursor.fetchall()]
        
        # Get overall stats
        cursor.execute('''
            SELECT 
                COUNT(*) as total_requests,
                SUM(total_tokens) as total_tokens,
                SUM(cost_usd) as total_cost,
                AVG(accuracy_score) as avg_accuracy,
                COUNT(DISTINCT user_ip) as unique_users,
                COUNT(DISTINCT file_name) as unique_files,
                AVG(response_time_ms) as avg_response_time
            FROM usage_logs
            WHERE timestamp >= ?
        ''', ((datetime.now() - timedelta(days=days)).isoformat(),))
        
        overall = dict(cursor.fetchone())
        
        conn.close()
        
        return {
            'overall': overall,
            'by_provider': provider_data,
            'period_days': days
        }
    
    def get_usage_summary(self) -> Dict[str, Any]:
        """Get comprehensive usage summary"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Today's stats
        today = datetime.now().date()
        cursor.execute('''
            SELECT 
                COUNT(*) as requests_today,
                SUM(total_tokens) as tokens_today,
                SUM(cost_usd) as cost_today
            FROM usage_logs
            WHERE DATE(timestamp) = ?
        ''', (today,))
        today_stats = dict(cursor.fetchone())
        
        # This month's stats
        month_start = datetime.now().replace(day=1).date()
        cursor.execute('''
            SELECT 
                COUNT(*) as requests_month,
                SUM(total_tokens) as tokens_month,
                SUM(cost_usd) as cost_month
            FROM usage_logs
            WHERE DATE(timestamp) >= ?
        ''', (month_start,))
        month_stats = dict(cursor.fetchone())
        
        # All-time stats
        cursor.execute('''
            SELECT 
                COUNT(*) as total_requests,
                SUM(total_tokens) as total_tokens,
                SUM(cost_usd) as total_cost,
                AVG(accuracy_score) as avg_accuracy,
                COUNT(DISTINCT user_ip) as unique_users
            FROM usage_logs
        ''')
        all_time = dict(cursor.fetchone())
        
        conn.close()
        
        return {
            'today': today_stats,
            'this_month': month_stats,
            'all_time': all_time
        }

# Example usage
if __name__ == "__main__":
    tracker = UsageTracker()
    
    # Log a sample usage
    result = tracker.log_usage(
        user_ip="127.0.0.1",
        file_name="test.yxmd",
        file_size=1024,
        workflow_name="Test Workflow",
        total_tools=5,
        provider="gemini",
        model="gemini-1.5-flash",
        input_text="Sample input text" * 100,
        output_text="Sample output text" * 200,
        response_time_ms=1500,
        success=True,
        improvements_count=3,
        accuracy_score=95
    )
    
    print("Logged usage:", result)
    
    # Get summary
    summary = tracker.get_usage_summary()
    print("\nUsage Summary:", json.dumps(summary, indent=2))