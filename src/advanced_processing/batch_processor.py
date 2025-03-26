# src/advanced_processing/batch_processor.py
import os
import logging
import pandas as pd
import numpy as np
import json
import time
import uuid
import threading
import queue
from typing import List, Dict, Any, Optional, Union, Tuple, Callable, Generator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing
from tqdm import tqdm
import traceback

class BatchProcessor:
    """
    Advanced batch processing system for handling large volumes of data
    with parallel processing, progress tracking, and error handling.
    """
    
    def __init__(self, max_workers: int = None, use_processes: bool = False, 
                 chunk_size: int = 100, log_dir: str = './logs'):
        """
        Initialize the batch processor.
        
        Args:
            max_workers (int, optional): Maximum number of worker threads/processes
            use_processes (bool): Whether to use processes instead of threads
            chunk_size (int): Default size of data chunks for batch processing
            log_dir (str): Directory for storing processing logs
        """
        self.max_workers = max_workers or min(32, (multiprocessing.cpu_count() * 2))
        self.use_processes = use_processes
        self.chunk_size = chunk_size
        self.log_dir = log_dir
        self.logger = logging.getLogger(__name__)
        
        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)
        
        # Storage for in-progress and completed batch jobs
        self.jobs = {}
        self._job_lock = threading.Lock()
        
        # Create a file handler for the logger
        file_handler = logging.FileHandler(os.path.join(log_dir, 'batch_processor.log'))
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
        # Set up result handling queue
        self._result_queue = queue.Queue()
        self._result_handler = threading.Thread(target=self._handle_results)
        self._result_handler.daemon = True
        self._result_handler.start()
    
    def process_dataframe(self, df: pd.DataFrame, processor_func: Callable, 
                          job_name: str = None, use_tqdm: bool = True, 
                          **processor_kwargs) -> str:
        """
        Process a DataFrame in batches with parallel execution.
        
        Args:
            df (pd.DataFrame): The DataFrame to process
            processor_func (Callable): Function to process each chunk
            job_name (str, optional): Name for this batch job
            use_tqdm (bool): Whether to display progress bar
            **processor_kwargs: Additional arguments to pass to processor_func
            
        Returns:
            str: Job ID for tracking the processing
        """
        if job_name is None:
            job_name = f"df_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
        job_id = str(uuid.uuid4())
        
        # Set up job tracking
        with self._job_lock:
            self.jobs[job_id] = {
                'name': job_name,
                'status': 'initializing',
                'created_at': datetime.now().isoformat(),
                'completed_chunks': 0,
                'total_chunks': 0,
                'errors': [],
                'results': [],
                'progress': 0.0,
                'processor': processor_func.__name__
            }
        
        # Generate chunks
        chunks = [df[i:i+self.chunk_size] for i in range(0, len(df), self.chunk_size)]
        total_chunks = len(chunks)
        
        with self._job_lock:
            self.jobs[job_id]['total_chunks'] = total_chunks
            self.jobs[job_id]['status'] = 'running'
        
        # Start processing in a separate thread
        threading.Thread(
            target=self._process_chunks,
            args=(chunks, processor_func, job_id, use_tqdm),
            kwargs=processor_kwargs
        ).start()
        
        return job_id
    
    def process_file_batches(self, file_list: List[str], processor_func: Callable,
                             job_name: str = None, use_tqdm: bool = True,
                             **processor_kwargs) -> str:
        """
        Process a list of files in batches with parallel execution.
        
        Args:
            file_list (List[str]): List of file paths to process
            processor_func (Callable): Function to process each file
            job_name (str, optional): Name for this batch job
            use_tqdm (bool): Whether to display progress bar
            **processor_kwargs: Additional arguments to pass to processor_func
            
        Returns:
            str: Job ID for tracking the processing
        """
        if job_name is None:
            job_name = f"file_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
        job_id = str(uuid.uuid4())
        
        # Set up job tracking
        with self._job_lock:
            self.jobs[job_id] = {
                'name': job_name,
                'status': 'initializing',
                'created_at': datetime.now().isoformat(),
                'completed_files': 0,
                'total_files': len(file_list),
                'errors': [],
                'results': [],
                'progress': 0.0,
                'processor': processor_func.__name__
            }
        
        # Generate chunks of files
        chunks = [file_list[i:i+self.chunk_size] for i in range(0, len(file_list), self.chunk_size)]
        
        with self._job_lock:
            self.jobs[job_id]['status'] = 'running'
        
        # Start processing in a separate thread
        threading.Thread(
            target=self._process_file_chunks,
            args=(chunks, processor_func, job_id, use_tqdm),
            kwargs=processor_kwargs
        ).start()
        
        return job_id
    
    def _process_chunks(self, chunks: List[pd.DataFrame], processor_func: Callable, 
                       job_id: str, use_tqdm: bool, **processor_kwargs) -> None:
        """
        Process DataFrame chunks in parallel.
        
        Args:
            chunks (List[pd.DataFrame]): List of DataFrame chunks
            processor_func (Callable): Function to process each chunk
            job_id (str): ID of the batch job
            use_tqdm (bool): Whether to display progress bar
            **processor_kwargs: Additional arguments to pass to processor_func
        """
        executor_class = ProcessPoolExecutor if self.use_processes else ThreadPoolExecutor
        results = []
        errors = []
        
        try:
            with executor_class(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_chunk = {
                    executor.submit(processor_func, chunk, **processor_kwargs): i 
                    for i, chunk in enumerate(chunks)
                }
                
                # Setup progress tracking
                if use_tqdm:
                    pbar = tqdm(total=len(chunks), desc=f"Processing {self.jobs[job_id]['name']}")
                
                # Process results as they complete
                for future in as_completed(future_to_chunk):
                    chunk_idx = future_to_chunk[future]
                    
                    try:
                        result = future.result()
                        results.append((chunk_idx, result))
                        self._result_queue.put(('chunk', job_id, chunk_idx, result, None))
                    except Exception as exc:
                        error_info = {
                            'chunk_idx': chunk_idx,
                            'error': str(exc),
                            'traceback': traceback.format_exc()
                        }
                        errors.append(error_info)
                        self._result_queue.put(('error', job_id, chunk_idx, None, error_info))
                    
                    # Update progress
                    with self._job_lock:
                        self.jobs[job_id]['completed_chunks'] += 1
                        self.jobs[job_id]['progress'] = self.jobs[job_id]['completed_chunks'] / len(chunks) * 100
                    
                    if use_tqdm:
                        pbar.update(1)
                
                if use_tqdm:
                    pbar.close()
        
        except Exception as exc:
            with self._job_lock:
                self.jobs[job_id]['status'] = 'failed'
                self.jobs[job_id]['error'] = str(exc)
                self.jobs[job_id]['traceback'] = traceback.format_exc()
            self.logger.error(f"Batch job {job_id} failed: {exc}")
            return
        
        # Mark job as completed
        with self._job_lock:
            self.jobs[job_id]['status'] = 'completed'
            self.jobs[job_id]['completed_at'] = datetime.now().isoformat()
            self.jobs[job_id]['duration'] = (
                datetime.fromisoformat(self.jobs[job_id]['completed_at']) - 
                datetime.fromisoformat(self.jobs[job_id]['created_at'])
            ).total_seconds()
        
        self.logger.info(f"Batch job {job_id} completed with {len(errors)} errors")
    
    def _process_file_chunks(self, chunks: List[List[str]], processor_func: Callable, 
                            job_id: str, use_tqdm: bool, **processor_kwargs) -> None:
        """
        Process file chunks in parallel.
        
        Args:
            chunks (List[List[str]]): List of file path chunks
            processor_func (Callable): Function to process each file
            job_id (str): ID of the batch job
            use_tqdm (bool): Whether to display progress bar
            **processor_kwargs: Additional arguments to pass to processor_func
        """
        executor_class = ProcessPoolExecutor if self.use_processes else ThreadPoolExecutor
        total_files = sum(len(chunk) for chunk in chunks)
        
        try:
            # Setup progress tracking
            if use_tqdm:
                pbar = tqdm(total=total_files, desc=f"Processing {self.jobs[job_id]['name']}")
            
            # Process each chunk of files
            for chunk_idx, file_chunk in enumerate(chunks):
                with executor_class(max_workers=self.max_workers) as executor:
                    # Submit batch of files
                    future_to_file = {
                        executor.submit(processor_func, file_path, **processor_kwargs): file_path 
                        for file_path in file_chunk
                    }
                    
                    # Process results as they complete
                    for future in as_completed(future_to_file):
                        file_path = future_to_file[future]
                        
                        try:
                            result = future.result()
                            self._result_queue.put(('file', job_id, file_path, result, None))
                        except Exception as exc:
                            error_info = {
                                'file': file_path,
                                'error': str(exc),
                                'traceback': traceback.format_exc()
                            }
                            self._result_queue.put(('error', job_id, file_path, None, error_info))
                        
                        # Update progress
                        with self._job_lock:
                            self.jobs[job_id]['completed_files'] += 1
                            self.jobs[job_id]['progress'] = self.jobs[job_id]['completed_files'] / total_files * 100
                        
                        if use_tqdm:
                            pbar.update(1)
                
            if use_tqdm:
                pbar.close()
        
        except Exception as exc:
            with self._job_lock:
                self.jobs[job_id]['status'] = 'failed'
                self.jobs[job_id]['error'] = str(exc)
                self.jobs[job_id]['traceback'] = traceback.format_exc()
            self.logger.error(f"Batch job {job_id} failed: {exc}")
            return
        
        # Mark job as completed
        with self._job_lock:
            self.jobs[job_id]['status'] = 'completed'
            self.jobs[job_id]['completed_at'] = datetime.now().isoformat()
            self.jobs[job_id]['duration'] = (
                datetime.fromisoformat(self.jobs[job_id]['completed_at']) - 
                datetime.fromisoformat(self.jobs[job_id]['created_at'])
            ).total_seconds()
        
        self.logger.info(f"File batch job {job_id} completed")
    
    def _handle_results(self) -> None:
        """Background thread to handle processing results and errors"""
        while True:
            try:
                result_type, job_id, item_id, result, error = self._result_queue.get()
                
                with self._job_lock:
                    if job_id not in self.jobs:
                        continue
                    
                    if result_type == 'error':
                        self.jobs[job_id]['errors'].append(error)
                    elif result_type in ('chunk', 'file'):
                        self.jobs[job_id]['results'].append((item_id, result))
                
                self._result_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error in result handler: {e}")
                time.sleep(0.1)  # Prevent tight loop in case of repeated errors
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get the status of a batch job.
        
        Args:
            job_id (str): ID of the job to check
            
        Returns:
            Dict[str, Any]: Job status information
        """
        with self._job_lock:
            if job_id not in self.jobs:
                return {'error': 'Job not found'}
            
            # Return a copy to avoid modification during read
            return dict(self.jobs[job_id])
    
    def list_jobs(self, status_filter: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List batch jobs with optional filtering.
        
        Args:
            status_filter (str, optional): Filter jobs by status
            limit (int): Maximum number of jobs to return
            
        Returns:
            List[Dict[str, Any]]: List of job information
        """
        with self._job_lock:
            jobs_list = []
            
            for job_id, job_info in self.jobs.items():
                if status_filter and job_info.get('status') != status_filter:
                    continue
                
                # Return a simplified view
                jobs_list.append({
                    'job_id': job_id,
                    'name': job_info.get('name'),
                    'status': job_info.get('status'),
                    'progress': job_info.get('progress', 0),
                    'created_at': job_info.get('created_at'),
                    'completed_at': job_info.get('completed_at', None),
                    'error_count': len(job_info.get('errors', []))
                })
            
            # Sort by creation time (descending) and limit results
            return sorted(jobs_list, key=lambda x: x['created_at'], reverse=True)[:limit]
    
    def get_job_results(self, job_id: str, as_dataframe: bool = False) -> Any:
        """
        Get the results of a completed batch job.
        
        Args:
            job_id (str): ID of the job
            as_dataframe (bool): Whether to return results as a DataFrame
            
        Returns:
            Any: Job results, format depends on the processor function
        """
        with self._job_lock:
            if job_id not in self.jobs:
                return {'error': 'Job not found'}
            
            job_info = self.jobs[job_id]
            
            if job_info['status'] not in ('completed', 'failed'):
                return {
                    'status': job_info['status'],
                    'progress': job_info['progress'],
                    'message': 'Job is still running or initializing'
                }
            
            # Extract results
            results = job_info.get('results', [])
            
            if not results:
                return {
                    'status': job_info['status'],
                    'message': 'No results available',
                    'error_count': len(job_info.get('errors', []))
                }
            
            # Sort results by chunk/file index
            sorted_results = sorted(results, key=lambda x: x[0])
            results_only = [r[1] for r in sorted_results]
            
            if as_dataframe and all(isinstance(r, pd.DataFrame) for r in results_only):
                # Combine DataFrames
                return pd.concat(results_only, ignore_index=True)
            else:
                return results_only
    
    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        """
        Cancel a running batch job.
        
        Args:
            job_id (str): ID of the job to cancel
            
        Returns:
            Dict[str, Any]: Cancellation result
        """
        with self._job_lock:
            if job_id not in self.jobs:
                return {'error': 'Job not found'}
            
            job_info = self.jobs[job_id]
            
            if job_info['status'] in ('completed', 'failed', 'cancelled'):
                return {
                    'message': f"Job already {job_info['status']}",
                    'status': job_info['status']
                }
            
            # Mark as cancelled
            job_info['status'] = 'cancelled'
            job_info['cancelled_at'] = datetime.now().isoformat()
        
        return {
            'message': 'Job cancelled',
            'status': 'cancelled',
            'job_id': job_id
        }
    
    def save_job_results(self, job_id: str, output_path: str, 
                        format: str = 'json') -> Dict[str, Any]:
        """
        Save job results to a file.
        
        Args:
            job_id (str): ID of the job
            output_path (str): Path to save results
            format (str): Output format (json, csv, xlsx)
            
        Returns:
            Dict[str, Any]: Result of the save operation
        """
        results = self.get_job_results(job_id)
        
        if isinstance(results, dict) and 'error' in results:
            return results
        
        try:
            if format == 'json':
                # For JSON, try to convert to serializable format
                serializable_results = self._make_serializable(results)
                with open(output_path, 'w') as f:
                    json.dump(serializable_results, f, indent=2)
            
            elif format in ('csv', 'xlsx'):
                # For tabular formats, convert to DataFrame if needed
                if not isinstance(results, pd.DataFrame):
                    df = self.get_job_results(job_id, as_dataframe=True)
                    if isinstance(df, dict) and 'error' in df:
                        return df
                else:
                    df = results
                
                if format == 'csv':
                    df.to_csv(output_path, index=False)
                else:  # xlsx
                    df.to_excel(output_path, index=False)
            
            else:
                return {'error': f'Unsupported format: {format}'}
            
            return {
                'message': f'Results saved to {output_path}',
                'format': format,
                'path': output_path
            }
            
        except Exception as e:
            error_info = {
                'error': str(e),
                'traceback': traceback.format_exc()
            }
            self.logger.error(f"Error saving job results: {e}")
            return error_info
    
    def _make_serializable(self, obj):
        """Convert object to JSON serializable format"""
        if isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        elif isinstance(obj, (datetime, np.datetime64)):
            return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
        elif isinstance(obj, (list, tuple)):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {str(k): self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, (pd.DataFrame, pd.Series)):
            return obj.to_dict()
        elif hasattr(obj, '__dict__'):
            return self._make_serializable(obj.__dict__)
        else:
            return str(obj)
    
    def create_generic_processor(self, process_func: Callable, 
                                error_handler: Optional[Callable] = None) -> Callable:
        """
        Create a generic processor function for batch processing.
        
        Args:
            process_func (Callable): Function to process each item
            error_handler (Callable, optional): Function to handle errors
            
        Returns:
            Callable: Processor function for batch processing
        """
        def processor(items, **kwargs):
            results = []
            errors = []
            
            for item in items:
                try:
                    result = process_func(item, **kwargs)
                    results.append(result)
                except Exception as e:
                    error = {
                        'item': item,
                        'error': str(e),
                        'traceback': traceback.format_exc()
                    }
                    errors.append(error)
                    
                    if error_handler:
                        error_handler(item, e, **kwargs)
            
            return {
                'results': results,
                'errors': errors,
                'total': len(items),
                'successful': len(results),
                'failed': len(errors)
            }
        
        return processor

    def streaming_process(self, data_generator: Generator, processor_func: Callable,
                         chunk_size: int = None, max_items: int = None,
                         **processor_kwargs) -> Generator[Dict[str, Any], None, None]:
        """
        Process data in a streaming fashion, yielding results as they're processed.
        
        Args:
            data_generator (Generator): Generator producing data items
            processor_func (Callable): Function to process each chunk
            chunk_size (int, optional): Size of chunks to process
            max_items (int, optional): Maximum number of items to process
            **processor_kwargs: Additional arguments for processor_func
            
        Yields:
            Dict[str, Any]: Processing results for each chunk
        """
        if chunk_size is None:
            chunk_size = self.chunk_size
        
        current_chunk = []
        item_count = 0
        
        for item in data_generator:
            current_chunk.append(item)
            item_count += 1
            
            if len(current_chunk) >= chunk_size:
                # Process the current chunk
                try:
                    result = processor_func(current_chunk, **processor_kwargs)
                    yield {
                        'status': 'success',
                        'chunk_size': len(current_chunk),
                        'items_processed': item_count,
                        'result': result
                    }
                except Exception as e:
                    yield {
                        'status': 'error',
                        'chunk_size': len(current_chunk),
                        'items_processed': item_count,
                        'error': str(e),
                        'traceback': traceback.format_exc()
                    }
                
                current_chunk = []
            
            if max_items and item_count >= max_items:
                break
        
        # Process any remaining items
        if current_chunk:
            try:
                result = processor_func(current_chunk, **processor_kwargs)
                yield {
                    'status': 'success',
                    'chunk_size': len(current_chunk),
                    'items_processed': item_count,
                    'result': result
                }
            except Exception as e:
                yield {
                    'status': 'error',
                    'chunk_size': len(current_chunk),
                    'items_processed': item_count,
                    'error': str(e),
                    'traceback': traceback.format_exc()
                }
