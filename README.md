# Advanced Python Scenario-Based Technical Assessment

## 1. Memory Management Deep Dive
You're debugging a Python service that processes scientific data and experiences memory issues. When processing large datasets (100+ GB), memory usage grows unbounded despite explicitly calling `gc.collect()`. A simplified version of the problematic code:

```python
class DataProcessor:
    def __init__(self):
        self.reference_data = load_large_reference_dataset()  # 2GB in memory
        self.processed_chunks = []
    
    def process_dataset(self, dataset_path):
        for chunk in self._chunk_reader(dataset_path):
            processed = self._process_chunk(chunk)
            self.processed_chunks.append(processed)
            
            # Attempt to free memory
            del chunk
            gc.collect()
        
        return self._consolidate_results()
    
    def _chunk_reader(self, path):
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(1024 * 1024 * 100)  # 100MB chunks
                if not chunk:
                    break
                yield self._parse_binary_chunk(chunk)
    
    def _parse_binary_chunk(self, binary_chunk):
        # Complex parsing logic that creates multiple intermediate objects
        # ...
        return parsed_data
    
    def _process_chunk(self, chunk):
        # Processing logic that involves self.reference_data
        # Creates multiple temporary dataframes and arrays
        # ...
        return result
    
    def _consolidate_results(self):
        return pd.concat(self.processed_chunks)
```

Identify all potential memory leak issues, explain how Python's memory management works in this context, and implement a comprehensive fix with memory profiling instrumentation.

## 2. Concurrent Processing Architecture
You need to build a distributed data processing pipeline that:
- Ingests data from 50+ different sources (REST APIs, databases, message queues)
- Performs complex transformations with interdependencies between data sources
- Handles backpressure when downstream systems slow down
- Recovers gracefully from failures at any stage
- Maximizes CPU utilization across 64-core machines
- Processes 10TB+ of data per day with strict latency requirements

Design a sophisticated Python architecture addressing these requirements, including appropriate libraries, synchronization mechanisms, error handling, and monitoring instrumentation.

## 3. Metaprogramming and Code Generation Challenge
Your team maintains a large codebase with hundreds of data model classes that need to be both database models (SQLAlchemy) and API schemas (Pydantic) with strict validation. Currently, you're duplicating a lot of code:

```python
# Database model
class UserDB(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    profile = relationship("Profile", back_populates="user")
    orders = relationship("Order", back_populates="user")

# API schema for the same entity
class UserSchema(BaseModel):
    id: Optional[int] = None
    email: EmailStr
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Nested models
    profile: Optional["ProfileSchema"] = None
    orders: List["OrderSchema"] = []
    
    class Config:
        orm_mode = True
```

Design a metaprogramming solution that:
- Generates both model types from a single source of truth
- Handles complex relationships (one-to-many, many-to-many)
- Supports different validation rules per field for different contexts
- Maintains type hints for IDE autocompletion
- Allows for custom behavior in specific models when needed

## 4. Advanced Algorithmic Optimization
Your team has implemented a function to find all paths between two nodes in a complex graph (representing a social network with millions of nodes). The current implementation is too slow for production:

```python
def find_all_paths(graph, start, end, path=None):
    path = path or [start]
    if start == end:
        return [path]
    
    if start not in graph:
        return []
    
    paths = []
    for node in graph[start]:
        if node not in path:  # Avoid cycles
            new_paths = find_all_paths(graph, node, end, path + [node])
            paths.extend(new_paths)
    
    return paths
```

Users need to find all possible paths between users subject to complex filtering rules (path length, node attributes, etc.). Implement an optimized solution with proper memory management that can:
- Work efficiently on large graphs (millions of nodes/edges)
- Utilize multiple CPU cores
- Apply configurable path filtering criteria
- Support early termination when finding a sufficient number of paths
- Include intelligent path prioritization based on user-defined heuristics

## 5. Advanced Testing Architecture
Your team is developing a complex distributed system with microservices, databases, message queues, and external API dependencies. Unit tests alone are insufficient, but full integration tests are slow and flaky. Design a comprehensive testing architecture that:
- Creates self-contained, reproducible test environments using Docker/Kubernetes
- Implements sophisticated mocking strategies for external dependencies
- Uses property-based testing for complex business logic
- Supports parameterized tests that can run against multiple database backends
- Includes performance benchmarking with statistical validation
- Integrates chaos engineering principles to test resilience
- Provides detailed coverage reports at various abstraction levels

Implement key components of your solution with explanations of design decisions.

## 6. Custom Context Managers for Resource Management
You're working on a high-security financial calculation engine that needs to:
- Access decrypted sensitive data only when absolutely necessary
- Ensure all copies of sensitive data are completely removed from memory after use
- Track and audit all access to sensitive data
- Apply proper concurrency controls to prevent race conditions
- Handle nested resource acquisition scenarios correctly
- Manage both software and hardware resources (GPU acceleration)

Implement a sophisticated context manager framework addressing these requirements, with proper error handling, cleanup, and audit logging.

## 7. Advanced Metaclass Applications
You're building a framework for scientific computing where users define complex computational models. Using metaclasses, implement a system that:
- Automatically validates mathematical constraints between model parameters
- Generates efficient C++ code for performance-critical sections
- Provides automatic differentiation for gradient calculations
- Implements smart caching based on parameter dependencies
- Tracks the computational graph for visualizing model structure
- Ensures thread safety when executing parallel computations

Explain when metaclasses are the appropriate solution versus alternatives, with concrete examples.

## 8. Sophisticated Decorator Framework
Design and implement a decorator framework for a mission-critical application that:
- Provides parameterized rate limiting with adaptive backoff strategies
- Implements circuit breaker patterns for external service calls
- Offers sophisticated caching with dependency invalidation
- Enforces complex access control rules based on user roles and resource ownership
- Tracks performance metrics with statistical aggregation
- Supports async functions correctly
- Can be composed with other decorators in a predictable way

Your solution should handle complex edge cases like function signature preservation, thread safety, and proper cleanup of resources.

## 9. Generator-Based Streaming Data Processing
You need to process a continuous stream of time-series data from IoT devices (millions of events per second), performing complex analytics while minimizing memory usage. Implement a generator-based pipeline that:
- Ingests data from multiple sources concurrently
- Performs windowed operations (sliding windows, tumbling windows)
- Detects anomalies using statistical models
- Dynamically adjusts processing based on data characteristics
- Checkpoints state for fault tolerance
- Optimizes CPU cache usage for numerical operations
- Supports backpressure mechanisms

Explain the memory usage patterns and optimization techniques in your solution.

## 10. Secure Dynamic Code Execution
You need to implement a rules engine where non-technical users can define business rules in a simplified Python-like language. These rules need to be compiled and executed in a production environment with strict security and performance requirements. Design a solution that:
- Safely parses and validates user-provided code
- Compiles rules to optimized bytecode or native code
- Enforces strict sandboxing to prevent malicious code execution
- Limits resource usage (CPU, memory, execution time)
- Provides detailed error messages for debugging
- Optimizes repeated execution of the same rules
- Supports versioning and rollback of rule changes

# DETAILED ANSWERS

## 1. Memory Management Deep Dive
**Answer:**

The code has several memory leak and inefficiency issues:

### Core Memory Issues

1. **Accumulating processed chunks**: `self.processed_chunks.append(processed)` stores all processed chunks in memory indefinitely.

2. **Reference cycles**: Complex objects may form reference cycles that the garbage collector doesn't immediately collect.

3. **Large reference data**: The 2GB reference dataset stays in memory for the entire processing duration.

4. **Hidden references**: Parsing logic may keep references to portions of the input data.

5. **Pandas operations**: `pd.concat()` creates a new dataframe, potentially doubling memory usage.

### Python Memory Management Mechanics

Python's memory management involves:

1. **Reference counting**: Python tracks how many references point to each object and frees memory when count reaches zero.

2. **Cycle detection**: The garbage collector periodically detects and breaks reference cycles, but it's not immediate.

3. **Memory fragmentation**: Even after objects are freed, memory may remain fragmented at the OS level.

4. **Numpy and Pandas memory**: These libraries often use memory outside Python's direct control.

### Comprehensive Solution:

```python
import gc
import os
import psutil
import tracemalloc
import pandas as pd
import numpy as np
from weakref import WeakValueDictionary
from contextlib import contextmanager
from memory_profiler import profile as memory_profile

class MemoryMonitor:
    """Monitor memory usage during processing"""
    def __init__(self, log_interval=5):
        self.process = psutil.Process(os.getpid())
        self.log_interval = log_interval
        self.baseline = None
        self.last_log_time = 0
        
    def start(self):
        """Start memory monitoring with tracemalloc"""
        tracemalloc.start(25)  # Capture 25 frames for each allocation
        self.baseline = self.get_memory_usage()
        self.last_log_time = time.time()
        return self
        
    def stop(self):
        """Stop monitoring and print summary"""
        current = tracemalloc.get_traced_memory()
        print(f"Current memory: {current[0] / 1024**2:.2f} MB, Peak: {current[1] / 1024**2:.2f} MB")
        print(f"Difference from baseline: {(self.get_memory_usage() - self.baseline) / 1024**2:.2f} MB")
        
        # Get top memory snapshots
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        print("Top 10 memory allocations:")
        for stat in top_stats[:10]:
            print(f"{stat}")
            
        tracemalloc.stop()
        
    def get_memory_usage(self):
        """Get current memory usage in bytes"""
        return self.process.memory_info().rss
    
    def check_and_log(self, step_name):
        """Log memory usage if interval has passed"""
        current_time = time.time()
        if current_time - self.last_log_time >= self.log_interval:
            current_usage = self.get_memory_usage()
            print(f"Memory at {step_name}: {current_usage / 1024**2:.2f} MB")
            
            # Get current snapshot difference
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')
            print(f"Top 3 allocations at {step_name}:")
            for stat in top_stats[:3]:
                print(f"  {stat}")
                
            self.last_log_time = current_time

@contextmanager
def tracked_memory(name):
    """Context manager to track memory for a specific operation"""
    gc.collect()  # Collect garbage before measuring
    process = psutil.Process(os.getpid())
    start_mem = process.memory_info().rss
    
    snapshot_start = tracemalloc.take_snapshot()
    start_time = time.time()
    
    try:
        yield
    finally:
        elapsed = time.time() - start_time
        current_mem = process.memory_info().rss
        diff = current_mem - start_mem
        
        snapshot_end = tracemalloc.take_snapshot()
        diff_stats = snapshot_end.compare_to(snapshot_start, 'lineno')
        
        print(f"\nMemory change during {name}: {diff / 1024**2:.2f} MB in {elapsed:.2f}s")
        for stat in diff_stats[:3]:
            print(f"  {stat}")

class DataProcessor:
    def __init__(self, cache_dir=None):
        self.cache_dir = cache_dir
        self.reference_data_file = self._prepare_reference_data()
        self._ref_data_cache = WeakValueDictionary()  # Weakref cache for reference data chunks
        self.monitor = MemoryMonitor()
        
    def _prepare_reference_data(self):
        """Save reference data to disk instead of keeping in memory"""
        ref_data = load_large_reference_dataset()
        
        if self.cache_dir:
            os.makedirs(self.cache_dir, exist_ok=True)
            file_path = os.path.join(self.cache_dir, 'reference_data.h5')
            
            # Save to HDF5 format for efficient partial reading
            with pd.HDFStore(file_path, mode='w') as store:
                store.put('reference_data', ref_data, format='table')
            
            return file_path
        else:
            # We'll create a temp file if no cache dir specified
            import tempfile
            temp_file = tempfile.NamedTemporaryFile(suffix='.h5', delete=False)
            with pd.HDFStore(temp_file.name, mode='w') as store:
                store.put('reference_data', ref_data, format='table')
            
            return temp_file.name
    
    def _get_reference_data_chunk(self, key_range):
        """Load only the necessary part of reference data"""
        cache_key = f"{key_range[0]}:{key_range[1]}"
        
        # Check if in weakref cache
        if cache_key in self._ref_data_cache:
            return self._ref_data_cache[cache_key]
        
        # Load from file if not cached
        with pd.HDFStore(self.reference_data_file, mode='r') as store:
            # Use where clause to load only required rows
            chunk = store.select('reference_data', 
                                where=f"index >= {key_range[0]} & index < {key_range[1]}")
            
            # Add to weakref cache
            self._ref_data_cache[cache_key] = chunk
            return chunk
    
    def process_dataset(self, dataset_path, output_path):
        """Process dataset chunk by chunk, writing results to disk incrementally"""
        self.monitor.start()
        
        try:
            # Create output file
            with pd.HDFStore(output_path, mode='w') as output_store:
                # Process in chunks with explicit memory management
                for chunk_num, chunk in enumerate(self._chunk_reader(dataset_path)):
                    chunk_name = f"chunk_{chunk_num}"
                    
                    with tracked_memory(f"processing chunk {chunk_num}"):
                        # Identify which part of reference data we need
                        ref_range = self._determine_reference_range(chunk)
                        ref_data_chunk = self._get_reference_data_chunk(ref_range)
                        
                        # Process the chunk
                        processed = self._process_chunk(chunk, ref_data_chunk)
                        
                        # Write immediately to disk instead of accumulating
                        output_store.put(chunk_name, processed, format='table')
                        
                    # Explicitly delete variables to help garbage collection
                    del chunk, ref_data_chunk, processed
                    
                    # Force garbage collection
                    gc.collect()
                    
                    # Log memory usage
                    self.monitor.check_and_log(f"after chunk {chunk_num}")
                
                # Consolidate results directly from disk
                self._consolidate_results(output_store)
            
            return output_path
            
        finally:
            self.monitor.stop()
            
            # Clean up temporary files
            if not self.cache_dir and os.path.exists(self.reference_data_file):
                os.unlink(self.reference_data_file)
    
    def _chunk_reader(self, path):
        """Generate parsed chunks from file with minimal memory usage"""
        # Use numpy's memmap for binary files to avoid loading entire file
        if path.endswith('.bin'):
            mm = np.memmap(path, dtype='float32', mode='r')
            chunk_size = 1024 * 1024 * 25  # 25MB chunks (in float32 elements)
            
            for i in range(0, len(mm), chunk_size):
                # Create a copy to avoid keeping a reference to the memmap
                chunk_copy = np.array(mm[i:i+chunk_size].copy())
                yield self._parse_binary_chunk(chunk_copy)
                
                # Explicitly delete to help garbage collection
                del chunk_copy
        else:
            # For text or other formats
            with open(path, 'rb') as f:
                while True:
                    with tracked_memory("reading chunk"):
                        chunk = f.read(1024 * 1024 * 100)  # 100MB chunks
                        
                    if not chunk:
                        break
                        
                    parsed = self._parse_binary_chunk(chunk)
                    yield parsed
                    
                    # Explicitly delete original chunk
                    del chunk
    
    def _parse_binary_chunk(self, binary_chunk):
        """Parse binary chunk with careful memory management"""
        with tracked_memory("parsing binary chunk"):
            # Use zero-copy operations where possible
            # For example, with numpy structured arrays:
            if isinstance(binary_chunk, np.ndarray):
                # Process directly without creating copies where possible
                parsed_data = self._process_numpy_array(binary_chunk)
            else:
                # For other binary formats
                parsed_data = self._parse_generic_binary(binary_chunk)
                
            return parsed_data
    
    def _determine_reference_range(self, chunk):
        """Determine which part of reference data is needed for this chunk"""
        # Implementation depends on data structure
        # Example:
        min_key = chunk['key'].min()
        max_key = chunk['key'].max()
        
        # Add buffer on both sides
        return (max(0, min_key - 10), max_key + 10)
    
    def _process_chunk(self, chunk, ref_data_chunk):
        """Process chunk with careful memory management"""
        # Use operations that modify in-place when possible
        result = pd.DataFrame()
        
        # Process in smaller sub-chunks if the data is large
        if len(chunk) > 10000:
            subchunk_size = 5000
            sub_results = []
            
            for i in range(0, len(chunk), subchunk_size):
                subchunk = chunk.iloc[i:i+subchunk_size]
                # Process subchunk
                processed_subchunk = self._process_subchunk(subchunk, ref_data_chunk)
                sub_results.append(processed_subchunk)
                
                # Delete subchunk to free memory
                del subchunk
            
            # Combine results
            result = pd.concat(sub_results, ignore_index=True)
            # Clear references to help GC
            del sub_results
        else:
            # Small chunk, process directly
            result = self._process_subchunk(chunk, ref_data_chunk)
        
        return result
    
    def _process_subchunk(self, subchunk, ref_data):
        """Process a manageable sized piece of data"""
        # Actual processing logic
        # Use inplace operations where possible
        # ...
        
        return processed_result
    
    def _consolidate_results(self, store):
        """Consolidate results directly from disk"""
        # Get all chunk names
        chunk_names = [key for key in store.keys() if key.startswith('/chunk_')]
        
        if not chunk_names:
            return pd.DataFrame()
        
        # Process chunks incrementally instead of loading all at once
        # For example, compute aggregations incrementally
        summary_stats = {}
        total_rows = 0
        
        for chunk_name in chunk_names:
            # Read chunk
            chunk = store.get(chunk_name)
            
            # Update statistics
            for col in chunk.columns:
                if col not in summary_stats:
                    summary_stats[col] = {
                        'sum': 0,
                        'sum_squares': 0,
                        'min': float('inf'),
                        'max': float('-inf')
                    }
                
                summary_stats[col]['sum'] += chunk[col].sum()
                summary_stats[col]['sum_squares'] += (chunk[col] ** 2).sum()
                summary_stats[col]['min'] = min(summary_stats[col]['min'], chunk[col].min())
                summary_stats[col]['max'] = max(summary_stats[col]['max'], chunk[col].max())
            
            total_rows += len(chunk)
            
            # Delete chunk reference
            del chunk
        
        # Calculate final statistics
        final_stats = pd.DataFrame({
            col: {
                'mean': stats['sum'] / total_rows,
                'std': np.sqrt(stats['sum_squares']/total_rows - (stats['sum']/total_rows)**2),
                'min': stats['min'],
                'max': stats['max']
            }
            for col, stats in summary_stats.items()
        })
        
        # Store the final statistics
        store.put('final_statistics', final_stats)
        
        return final_stats
```

### Key Memory Optimization Strategies:

1. **Streaming processing**: Process one chunk at a time, never keeping the full dataset in memory.

2. **Disk-based storage**: Store intermediate results on disk using formats like HDF5.

3. **Explicit garbage collection**: Strategic calls to `gc.collect()` after processing steps.

4. **Memory profiling**: Integrated monitoring to track memory usage throughout processing.

5. **WeakRef caching**: Use `WeakValueDictionary` to cache reference data without preventing garbage collection.

6. **Zero-copy operations**: Use numpy views and in-place operations where possible.

7. **Memory-mapped files**: Use `np.memmap` for large binary files to avoid loading completely into memory.

8. **Sub-chunking**: Break large operations into smaller pieces to control memory spikes.

9. **Context tracking**: Use context managers to precisely track memory usage of specific operations.

10. **Incremental result consolidation**: Process final results incrementally instead of loading all intermediate results.

This architecture provides both a solution to the immediate memory leaks and a framework for ongoing memory profiling and management.

## 2. Concurrent Processing Architecture
**Answer:**

I'll design a scalable, resilient architecture for the distributed data processing system with high throughput and fault tolerance.

### System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Distributed Processing Architecture              │
├─────────┬───────────┬─────────────┬───────────────┬─────────────────────┤
│         │           │             │               │                     │
│  Data   │  Ingest   │  Processing │  Storage &    │  Monitoring &       │
│ Sources │  Layer    │  Layer      │  Output       │  Management         │
│         │           │             │               │                     │
├─────────┼───────────┼─────────────┼───────────────┼─────────────────────┤
│         │           │             │               │                     │
│ REST    │ Async     │ Distributed │ Time-series   │ Prometheus          │
│ APIs    │ Workers   │ Task Queue  │ DB            │ Metrics             │
│         │           │             │               │                     │
│ RDBMS   │ Rate      │ Worker      │ Object        │ Distributed         │
│         │ Limiters  │ Pools       │ Storage       │ Tracing             │
│         │           │             │               │                     │
│ Message │ Circuit   │ DAG         │ Stream        │ Centralized         │
│ Queues  │ Breakers  │ Executor    │ Processing    │ Logging             │
│         │           │             │               │                     │
│ Files   │ Retry     │ Backpressure│ Cache         │ Alerting            │
│         │ Logic     │ Control     │ Layer         │                     │
│         │           │             │               │                     │
└─────────┴───────────┴─────────────┴───────────────┴─────────────────────┘
```

### Core Components Implementation

```python
import asyncio
import time
import logging
import signal
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Set, Optional, Any, Callable, Union, Tuple, AsyncGenerator
import uuid
import functools
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp
import numpy as np
import pandas as pd
import aiohttp
import aiobotocore.session
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
import asyncpg
import redis.asyncio as aioredis
import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import distributed  # Dask distributed
import ray
from prefect import task, flow
import networkx as nx

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger()

# Metrics
INGEST_COUNTER = Counter('data_ingest_total', 'Total data sources ingested', ['source_type'])
PROCESSING_DURATION = Histogram('processing_duration_seconds', 
                               'Time spent processing data', 
                               ['stage', 'success'])
MEMORY_USAGE = Gauge('memory_usage_bytes', 'Memory usage by component', 
                    ['component'])
ERROR_COUNTER = Counter('processing_errors_total', 'Processing errors', 
                       ['source', 'error_type'])
BACKPRESSURE_GAUGE = Gauge('backpressure_level', 'Backpressure level by component', 
                          ['component'])

class DataSourceType(Enum):
    """Types of data sources supported by the system"""
    REST_API = "rest_api"
    DATABASE = "database"
    MESSAGE_QUEUE = "message_queue"
    FILE = "file"
    STREAM = "stream"

class ProcessingState(Enum):
    """States for a processing task"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRY = "retry"

@dataclass
class CircuitBreakerConfig:
    """Configuration for a circuit breaker"""
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_timeout: float = 5.0

class CircuitBreaker:
    """Circuit breaker implementation for external dependencies"""
    def __init__(self, name: str, config: CircuitBreakerConfig = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.failures = 0
        self.state = "closed"  # closed, open, half-open
        self.last_failure_time = 0
        self.lock = asyncio.Lock()
    
    async def __aenter__(self):
        await self.before_call()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.after_call(exc_type is not None)
        return False  # Don't suppress exceptions
    
    async def before_call(self):
        """Check if the circuit is closed before making a call"""
        async with self.lock:
            now = time.time()
            
            if self.state == "open":
                if now - self.last_failure_time > self.config.recovery_timeout:
                    log.info("circuit_half_open", circuit=self.name)
                    self.state = "half-open"
                else:
                    raise CircuitOpenError(f"Circuit {self.name} is open")
            
            if self.state == "half-open" and now - self.last_failure_time < self.config.half_open_timeout:
                raise CircuitOpenError(f"Circuit {self.name} is half-open and cooling down")
    
    async def after_call(self, failed: bool):
        """Update circuit state after a call"""
        async with self.lock:
            if failed:
                self.failures += 1
                self.last_failure_time = time.time()
                
                if self.state == "half-open" or self.failures >= self.config.failure_threshold:
                    log.warning("circuit_opened", circuit=self.name, failures=self.failures)
                    self.state = "open"
            else:
                if self.state == "half-open":
                    log.info("circuit_closed", circuit=self.name)
                    self.state = "closed"
                
                self.failures = 0

class CircuitOpenError(Exception):
    """Error raised when a circuit is open"""
    pass

class BackpressureController:
    """Controls backpressure to prevent resource exhaustion"""
    def __init__(self, name: str, max_concurrent: int = 100, 
                 cooldown_factor: float = 0.8, 
                 scale_factor: float = 1.5):
        self.name = name
        self.max_concurrent = max_concurrent
        self.current = 0
        self.cooldown_factor = cooldown_factor
        self.scale_factor = scale_factor
        self.lock = asyncio.Lock()
        self.paused = False
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Update metrics
        BACKPRESSURE_GAUGE.labels(component=name).set(0)
    
    async def acquire(self):
        """Acquire permission to process another item"""
        # Quick non-blocking check before waiting on semaphore
        if self.paused:
            raise BackpressureExceededError(f"Component {self.name} is paused due to backpressure")
        
        await self.semaphore.acquire()
        async with self.lock:
            self.current += 1
            pressure = self.current / self.max_concurrent
            BACKPRESSURE_GAUGE.labels(component=self.name).set(pressure)
    
    async def release(self):
        """Release a processing slot"""
        async with self.lock:
            self.current -= 1
            pressure = max(0, self.current / self.max_concurrent)
            BACKPRESSURE_GAUGE.labels(component=self.name).set(pressure)
            
            # If we've reduced load enough and were paused, unpause