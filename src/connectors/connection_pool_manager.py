"""
Connection Pool Manager for Database Connectors.

This module provides centralized connection pool management for database connectors,
enabling resource optimization, cleanup, and performance improvements across the application.
"""

import threading
import time
import logging
from typing import Any, Dict, List, Optional, Type, Union
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum

from .database_connector import DatabaseConnector
from .postgresql_connector import PostgreSQLConnector
from .mysql_connector import MySQLConnector
from .sqlite_connector import SQLiteConnector


class PoolStatus(Enum):
    """Enumeration for connection pool status."""
    ACTIVE = "active"
    IDLE = "idle"
    CLOSED = "closed"
    ERROR = "error"


@dataclass
class PoolMetrics:
    """Data class for connection pool metrics."""
    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    failed_connections: int = 0
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    average_response_time: float = 0.0
    created_at: float = 0.0
    last_used: float = 0.0


class ConnectionPoolManager:
    """
    Centralized connection pool manager for database connectors.
    
    This class manages multiple database connection pools, providing resource optimization,
    cleanup, monitoring, and performance improvements for database operations.
    
    Attributes:
        pools: Dictionary of active connection pools
        pool_configs: Configuration for each pool
        pool_metrics: Metrics tracking for each pool
        cleanup_thread: Background thread for pool maintenance
        shutdown_event: Event to signal shutdown
        lock: Thread lock for pool operations
    """
    
    def __init__(self, cleanup_interval: int = 300):
        """
        Initialize the connection pool manager.
        
        Args:
            cleanup_interval: Interval in seconds for pool cleanup operations
        """
        self.pools: Dict[str, DatabaseConnector] = {}
        self.pool_configs: Dict[str, Dict[str, Any]] = {}
        self.pool_metrics: Dict[str, PoolMetrics] = {}
        self.pool_status: Dict[str, PoolStatus] = {}
        
        self.cleanup_interval = cleanup_interval
        self.shutdown_event = threading.Event()
        self.lock = threading.RLock()
        
        self.logger = logging.getLogger('data_ingestion.connection_pool_manager')
        
        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True)
        self.cleanup_thread.start()
        
        self.logger.info("Connection pool manager initialized")
    
    def create_pool(self, pool_name: str, db_type: str, connection_params: Dict[str, Any],
                   max_connections: int = 10, min_connections: int = 1,
                   connection_timeout: int = 30, pool_timeout: int = 300) -> bool:
        """
        Create a new connection pool.
        
        Args:
            pool_name: Unique name for the connection pool
            db_type: Type of database (postgresql, mysql, sqlite)
            connection_params: Database connection parameters
            max_connections: Maximum number of connections in the pool
            min_connections: Minimum number of connections to maintain
            connection_timeout: Timeout for individual connections
            pool_timeout: Timeout for getting connection from pool
            
        Returns:
            bool: True if pool was created successfully
            
        Raises:
            ValueError: If pool already exists or invalid parameters
        """
        with self.lock:
            if pool_name in self.pools:
                raise ValueError(f"Pool '{pool_name}' already exists")
            
            try:
                # Create connector based on database type
                connector = self._create_connector(db_type, connection_params)
                
                # Test connection
                if not connector.connect():
                    raise Exception("Failed to establish initial connection")
                
                # Store pool configuration
                pool_config = {
                    'db_type': db_type,
                    'connection_params': connection_params,
                    'max_connections': max_connections,
                    'min_connections': min_connections,
                    'connection_timeout': connection_timeout,
                    'pool_timeout': pool_timeout
                }
                
                self.pools[pool_name] = connector
                self.pool_configs[pool_name] = pool_config
                self.pool_metrics[pool_name] = PoolMetrics(
                    total_connections=1,
                    active_connections=1,
                    created_at=time.time(),
                    last_used=time.time()
                )
                self.pool_status[pool_name] = PoolStatus.ACTIVE
                
                self.logger.info(f"Created connection pool '{pool_name}' for {db_type}")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to create pool '{pool_name}': {str(e)}")
                return False
    
    def _create_connector(self, db_type: str, connection_params: Dict[str, Any]) -> DatabaseConnector:
        """
        Create a database connector based on type.
        
        Args:
            db_type: Type of database
            connection_params: Connection parameters
            
        Returns:
            DatabaseConnector: Appropriate connector instance
            
        Raises:
            ValueError: If database type is not supported
        """
        db_type = db_type.lower()
        
        if db_type == 'postgresql':
            return PostgreSQLConnector(connection_params)
        elif db_type == 'mysql':
            return MySQLConnector(connection_params)
        elif db_type == 'sqlite':
            return SQLiteConnector(connection_params)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    @contextmanager
    def get_connection(self, pool_name: str):
        """
        Get a connection from the specified pool.
        
        Args:
            pool_name: Name of the connection pool
            
        Yields:
            DatabaseConnector: Database connector instance
            
        Raises:
            ValueError: If pool doesn't exist
            TimeoutError: If connection cannot be obtained within timeout
        """
        if pool_name not in self.pools:
            raise ValueError(f"Pool '{pool_name}' does not exist")
        
        start_time = time.time()
        
        try:
            with self.lock:
                connector = self.pools[pool_name]
                metrics = self.pool_metrics[pool_name]
                
                # Update metrics
                metrics.total_requests += 1
                metrics.last_used = time.time()
                metrics.active_connections += 1
                
                self.logger.debug(f"Retrieved connection from pool '{pool_name}'")
            
            yield connector
            
            # Mark request as successful
            with self.lock:
                metrics.successful_requests += 1
                response_time = time.time() - start_time
                
                # Update average response time
                if metrics.successful_requests > 1:
                    metrics.average_response_time = (
                        (metrics.average_response_time * (metrics.successful_requests - 1) + response_time) /
                        metrics.successful_requests
                    )
                else:
                    metrics.average_response_time = response_time
                
        except Exception as e:
            # Mark request as failed
            with self.lock:
                metrics.failed_requests += 1
                self.logger.error(f"Error using connection from pool '{pool_name}': {str(e)}")
            raise
        finally:
            # Return connection to pool
            with self.lock:
                if pool_name in self.pool_metrics:
                    self.pool_metrics[pool_name].active_connections -= 1
    
    def remove_pool(self, pool_name: str) -> bool:
        """
        Remove and cleanup a connection pool.
        
        Args:
            pool_name: Name of the pool to remove
            
        Returns:
            bool: True if pool was removed successfully
        """
        with self.lock:
            if pool_name not in self.pools:
                self.logger.warning(f"Pool '{pool_name}' does not exist")
                return False
            
            try:
                # Disconnect the connector
                connector = self.pools[pool_name]
                connector.disconnect()
                
                # Remove from tracking
                del self.pools[pool_name]
                del self.pool_configs[pool_name]
                del self.pool_metrics[pool_name]
                del self.pool_status[pool_name]
                
                self.logger.info(f"Removed connection pool '{pool_name}'")
                return True
                
            except Exception as e:
                self.logger.error(f"Error removing pool '{pool_name}': {str(e)}")
                return False
    
    def get_pool_metrics(self, pool_name: str = None) -> Union[PoolMetrics, Dict[str, PoolMetrics]]:
        """
        Get metrics for a specific pool or all pools.
        
        Args:
            pool_name: Name of the pool (None for all pools)
            
        Returns:
            PoolMetrics or Dict of metrics
        """
        with self.lock:
            if pool_name:
                if pool_name not in self.pool_metrics:
                    raise ValueError(f"Pool '{pool_name}' does not exist")
                return self.pool_metrics[pool_name]
            else:
                return dict(self.pool_metrics)
    
    def get_pool_status(self, pool_name: str = None) -> Union[PoolStatus, Dict[str, PoolStatus]]:
        """
        Get status for a specific pool or all pools.
        
        Args:
            pool_name: Name of the pool (None for all pools)
            
        Returns:
            PoolStatus or Dict of statuses
        """
        with self.lock:
            if pool_name:
                if pool_name not in self.pool_status:
                    raise ValueError(f"Pool '{pool_name}' does not exist")
                return self.pool_status[pool_name]
            else:
                return dict(self.pool_status)
    
    def health_check(self, pool_name: str = None) -> Dict[str, Any]:
        """
        Perform health check on pools.
        
        Args:
            pool_name: Name of the pool (None for all pools)
            
        Returns:
            Dict containing health check results
        """
        results = {}
        pools_to_check = [pool_name] if pool_name else list(self.pools.keys())
        
        for name in pools_to_check:
            if name not in self.pools:
                results[name] = {'status': 'not_found'}
                continue
            
            try:
                connector = self.pools[name]
                test_result, error_msg = connector.test_connection()
                
                results[name] = {
                    'status': 'healthy' if test_result else 'unhealthy',
                    'error': error_msg,
                    'metrics': self.pool_metrics[name],
                    'last_used': self.pool_metrics[name].last_used,
                    'uptime': time.time() - self.pool_metrics[name].created_at
                }
                
                # Update pool status
                with self.lock:
                    self.pool_status[name] = PoolStatus.ACTIVE if test_result else PoolStatus.ERROR
                    
            except Exception as e:
                results[name] = {
                    'status': 'error',
                    'error': str(e),
                    'metrics': self.pool_metrics.get(name),
                }
                
                with self.lock:
                    self.pool_status[name] = PoolStatus.ERROR
        
        return results
    
    def _cleanup_worker(self):
        """Background worker for pool cleanup and maintenance."""
        while not self.shutdown_event.wait(self.cleanup_interval):
            try:
                self._perform_cleanup()
            except Exception as e:
                self.logger.error(f"Error during pool cleanup: {str(e)}")
    
    def _perform_cleanup(self):
        """Perform cleanup operations on all pools."""
        current_time = time.time()
        
        with self.lock:
            for pool_name in list(self.pools.keys()):
                try:
                    metrics = self.pool_metrics[pool_name]
                    config = self.pool_configs[pool_name]
                    
                    # Check for idle pools
                    idle_time = current_time - metrics.last_used
                    if idle_time > config.get('pool_timeout', 300):
                        self.pool_status[pool_name] = PoolStatus.IDLE
                        self.logger.debug(f"Pool '{pool_name}' marked as idle (idle for {idle_time:.1f}s)")
                    
                    # Perform health check periodically
                    if idle_time > 60:  # Health check every minute for idle pools
                        self.health_check(pool_name)
                        
                except Exception as e:
                    self.logger.error(f"Error during cleanup of pool '{pool_name}': {str(e)}")
    
    def shutdown(self):
        """Shutdown the pool manager and cleanup all resources."""
        self.logger.info("Shutting down connection pool manager")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        # Wait for cleanup thread to finish
        if self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=5)
        
        # Close all pools
        with self.lock:
            for pool_name in list(self.pools.keys()):
                self.remove_pool(pool_name)
        
        self.logger.info("Connection pool manager shutdown complete")
    
    def __del__(self):
        """Destructor to ensure proper cleanup."""
        try:
            self.shutdown()
        except:
            pass


# Global pool manager instance
_pool_manager = None
_pool_manager_lock = threading.Lock()


def get_pool_manager() -> ConnectionPoolManager:
    """
    Get the global connection pool manager instance.
    
    Returns:
        ConnectionPoolManager: Global pool manager instance
    """
    global _pool_manager
    
    if _pool_manager is None:
        with _pool_manager_lock:
            if _pool_manager is None:
                _pool_manager = ConnectionPoolManager()
    
    return _pool_manager


def shutdown_pool_manager():
    """Shutdown the global pool manager."""
    global _pool_manager
    
    if _pool_manager is not None:
        with _pool_manager_lock:
            if _pool_manager is not None:
                _pool_manager.shutdown()
                _pool_manager = None