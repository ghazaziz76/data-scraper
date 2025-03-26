import os
from datetime import datetime

class FileManager:
    @staticmethod
    def create_project_directory(base_path: str = 'projects'):
        """
        Create a new project directory with timestamp

        :param base_path: Base directory for projects
        :return: Path to the new project directory
        """
        # Create base projects directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Create timestamped project directory
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        project_dir = os.path.join(base_path, f'scraper_project_{timestamp}')

        os.makedirs(project_dir, exist_ok=True)
        os.makedirs(os.path.join(project_dir, 'raw'), exist_ok=True)
        os.makedirs(os.path.join(project_dir, 'output'), exist_ok=True)

        return project_dir

    @staticmethod
    def save_raw_html(html_content: str, project_dir: str, filename: str = 'page.html'):
        """
        Save raw HTML content to a file

        :param html_content: HTML content to save
        :param project_dir: Directory to save the file
        :param filename: Name of the file to save
        """
        raw_dir = os.path.join(project_dir, 'raw')
        filepath = os.path.join(raw_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)

        return filepath

