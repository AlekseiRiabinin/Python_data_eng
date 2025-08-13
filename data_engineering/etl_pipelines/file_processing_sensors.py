import os
import subprocess
from datetime import datetime, timedelta
from typing import Optional, Callable, Any, Self
from airflow.models import BaseOperator, BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from pathlib import Path


class CustomFileSensor(BaseSensorOperator):
    """Enhanced File Sensor."""
    
    template_fields = ('filepaths',)

    @apply_defaults
    def __init__(
        self: Self,
        filepaths: str | list[str],
        check_content: Optional[Callable[[str], bool]] = None,
        min_file_size: Optional[int] = None,
        file_pattern: Optional[str] = None,
        recursive: bool = False,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.filepaths = [filepaths] if isinstance(filepaths, str) else filepaths
        self.check_content = check_content
        self.min_file_size = min_file_size
        self.file_pattern = file_pattern
        self.recursive = recursive

    def _check_file(self: Self, filepath: str) -> bool:
        """Check if file meets all criteria."""
        try:
            path = Path(filepath)
            
            # Check file exists
            if not path.exists():
                return False
                
            # Check file size if specified
            if self.min_file_size and path.stat().st_size < self.min_file_size:
                self.log.info(f"File too small: {filepath}")
                return False
                
            # Check content if specified
            if self.check_content:
                with open(filepath, 'r') as f:
                    if not self.check_content(f.read()):
                        self.log.info(f"Content validation failed for: {filepath}")
                        return False
            return True
            
        except Exception as e:
            self.log.error(f"Error checking file {filepath}: {str(e)}")
            return False

    def poke(self: Self, context: Any) -> bool:
        """Check for files matching criteria."""
        found_files = []
        
        for filepath in self.filepaths:
            # Handle directory patterns
            if '*' in filepath or self.recursive:
                parent_dir = os.path.dirname(filepath)
                pattern = os.path.basename(filepath)
                
                if not os.path.exists(parent_dir):
                    continue
                    
                for root, _, files in os.walk(parent_dir):
                    for f in files:
                        full_path = os.path.join(root, f)
                        if self._check_file(full_path):
                            found_files.append(full_path)

                    if not self.recursive:
                        break
            else:
                if self._check_file(filepath):
                    found_files.append(filepath)
        
        if found_files:
            context['ti'].xcom_push(key='found_files', value=found_files)
            return True
            
        return False


class EnhancedBashOperator(BaseOperator):
    """Enhanced Bash Operator."""
    
    template_fields = ('bash_command', 'env')
    
    @apply_defaults
    def __init__(
        self: Self,
        bash_command: str,
        env: Optional[dict] = None,
        output_encoding: str = 'utf-8',
        skip_on_exit_code: Optional[int] = None,
        cwd: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.output_encoding = output_encoding
        self.skip_on_exit_code = skip_on_exit_code
        self.cwd = cwd

    def execute(self: Self, context: Any) -> Optional[str]:
        """Execute the bash command and return output if any."""
        self.log.info(f"Executing command: {self.bash_command}")

        # Merge with existing environment variables
        env = os.environ.copy()
        if self.env:
            env.update(self.env)
        
        try:
            result = subprocess.run(
                self.bash_command,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding=self.output_encoding,
                env=env,
                cwd=self.cwd
            )

            # Log output
            if result.stdout:
                self.log.info(f"Command output:\n{result.stdout}")
            if result.stderr:
                self.log.warning(f"Command stderr:\n{result.stderr}")
                
            return result.stdout
            
        except subprocess.CalledProcessError as e:
            if e.returncode == self.skip_on_exit_code:
                self.log.info(f"Skipping as command returned exit code {e.returncode}")
                return None

            self.log.error(f"Command failed with exit code {e.returncode}")
            self.log.error(f"Error output:\n{e.stderr}")
            raise


# Example DAG using custom operators
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def validate_file_content(content: str) -> bool:
    """Example content validation function."""
    return "transaction_id" in content.lower()


with DAG(
    'enhanced_file_processing',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    wait_for_file = CustomFileSensor(
        task_id='wait_for_file',
        filepaths=['/data/incoming/sales_{{ ds }}.csv'],
        check_content=validate_file_content,
        min_file_size=1024,
        poke_interval=30,
        timeout=60*60,
        mode='poke'
    )
    
    validate_file = EnhancedBashOperator(
        task_id='validate_file',
        bash_command='python /scripts/validate.py /data/incoming/sales_{{ ds }}.csv',
        env={'PYTHONPATH': '/scripts'},
        skip_on_exit_code=10
    )
    
    process_file = EnhancedBashOperator(
        task_id='process_file',
        bash_command='python /scripts/process.py /data/incoming/sales_{{ ds }}.csv',
        cwd='/scripts'
    )
    
    archive_file = EnhancedBashOperator(
        task_id='archive_file',
        bash_command='mv /data/incoming/sales_{{ ds }}.csv /data/archive/'
    )

    wait_for_file >> validate_file >> process_file >> archive_file
